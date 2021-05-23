import struct
from enum import Enum, unique
from dataclasses import dataclass


@unique
class MessageType(Enum):
    NONE = 0
    HELLO = 1
    GOODBYE = 2
    STATUSCHG = 3
    DATA = 4


class BaseMessage:
    """
    ABC for messages
    """

    @property
    def type(self):
        return self._type

    def pack(self):
        raise NotImplementedError(f"Class {self} does not override pack()")


@dataclass
class MessageFrame:
    header_fmt = "!IB"
    header_len = struct.calcsize(header_fmt)
    _type: MessageType = MessageType.NONE
    data: bytes = b''


    @property
    def type(self):
        return self._type


    def pack(self):
        fmt = self.header_fmt + str(len(self.data)) + 's'
        return struct.pack(fmt, len(self.data), self.type.value, self.data)


    @classmethod
    def unpack(cls, buf):
        if len(buf) < cls.header_len:
            # print("Incomplete header passed to MessageFrame.unpack()")
            return None

        length, mtype = struct.unpack(cls.header_fmt, buf[:cls.header_len])
        if len(buf) - cls.header_len < length:
            # print(f"Incomplete message received for buffer of type {MessageType(mtype).name} and length {length}")
            return None

        data = struct.unpack_from(str(length) + 's', buf, cls.header_len)
        return cls(mtype, data[0])


    @classmethod
    def unpack_data(cls, hdr, buf):
        if len(hdr) < cls.header_len:
            # print("Incomplete header passed to MessageFrame.unpack()")
            return None
        length, mtype = struct.unpack(cls.header_fmt, hdr)
        if len(buf) < length:
            # print(f"Incomplete message received for buffer of type {MessageType(mtype).name} and length {length}")
            return None

        data = struct.unpack_from(str(length) + 's', buf, 0)
        return cls(mtype, data[0])


    @classmethod
    def get_message_size(cls, buf):
        if len(buf) < cls.header_len:
            # print("Incomplete header passed to MessageFrame.unpack()")
            return -1

        length, mtype = struct.unpack(cls.header_fmt, buf[:cls.header_len])
        return length


    @classmethod
    def compose(cls, msg:BaseMessage):
        """
        Quick way to create a transmittable message - A message frame with a
        message as payload.
        """
        payload = msg.pack()
        return cls(msg.type, payload).pack()



class HelloMessage(BaseMessage):
    """
    Message type for introducing a new job or a new host to the server
    """
    _type = MessageType.HELLO


    def __init__(self, host_name, job_name):
        """
        Create a new hello message type, for transmission
        """
        self._host_name = host_name
        self._job_name = job_name


    @property
    def host_name(self):
        return self._host_name


    @property
    def job_name(self):
        return self._job_name


    def pack(self):
        """
        Pack this object into a bytearray for transmission.
        """
        return serialize_string_list([self.host_name, self.job_name])


    @classmethod
    def from_msg_frame(cls, msgframe):
        """
        Deserialize a hello message from a message frame object.
        """
        host_name, job_name = deserialize_string_list(msgframe.data)
        return cls(host_name, job_name)


class GoodbyeMessage(BaseMessage):
    """
    Message type for declaring that a job or host has stopped.
    Not specifying a specific job name indicates that the host (and all jobs on
    it) have terminated.
    """
    _type = MessageType.GOODBYE


    def __init__(self, host_name, job_name=None):
        self._host_name = host_name
        self._job_name = job_name


    @property
    def host_name(self):
        return self._host_name


    @property
    def job_name(self):
        return self._job_name


    def pack(self):
        """
        Pack this object into a bytearray for transmission.
        """
        if self.job_name is not None:
            return serialize_string_list([self.host_name, self.job_name])
        else:
            return serialize_string_list([self.host_name])


    @classmethod
    def from_msg_frame(cls, msgframe):
        """
        Deserialize a goodbye  message from a message frame object.
        """
        strs = deserialize_string_list(msgframe.data)
        if len(strs) == 2:
            return cls(strs[0], strs[1])
        else:
            return cls(strs[0], None)


class StatusChangeMessage(BaseMessage):
    """
    Message type for indicating some kind of run status change, such as
    completion, failure, awaiting input, etc. Additionally, used to change
    various attributes about the job/host, such as visibility.
    """


### Module-level helper functions (can you tell I'm a C programmer ??!?)


def serialize_string_list(strs, do_pack=True):
    """
    Serialize a list of strings.
    strs: strings to encode and serialize
    do_pack: When True (default), returns a bytearray. When False, returns
    the format string and the lengths + encoded strings, for use in packing.
    """
    fmt = '!'
    pack_items = list()
    for string in strs:
        byte_str = string.encode('utf-8')
        byte_str_len = len(byte_str)
        pack_items.append(byte_str_len)
        pack_items.append(byte_str)
        if len(byte_str) > (2**32)-1:
            raise RuntimeError("Way, WAY too big of a string passed in for serialization.")
        fmt += 'I' + str(byte_str_len) + 's'

    if do_pack:
        return struct.pack(fmt, *pack_items)
    else:
        return fmt, byte_strs


def deserialize_string_list(arr):
    """
    Deserialize a list of strings, in the format !IS...
    arr: bytearray to decode.
    returns: list of strings.
    """
    strings = list()
    total_bytes = len(arr)
    bytes_read = 0
    uint32_t_size = struct.calcsize("!I")
    while bytes_read < total_bytes:
        next_str_len = struct.unpack_from("!I", arr, bytes_read)[0]
        bytes_read += uint32_t_size
        next_byte_str = struct.unpack_from(
            str(next_str_len) + 's',
            arr,
            bytes_read
        )[0]
        bytes_read += next_str_len
        strings.append(next_byte_str.decode('utf-8'))

    return strings


