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
        job_name_s = self.job_name.encode('utf-8')
        host_name_s = self.host_name.encode('utf-8')
        job_name_bytes = len(job_name_s)
        host_name_bytes = len(host_name_s)
        return struct.pack(
            '!I' + str(job_name_bytes) + 'sI' + str(host_name_bytes) + 's',
            job_name_bytes, job_name_s,
            host_name_bytes, host_name_s
        )


    @classmethod
    def from_msg_frame(cls, msgframe):
        """
        Deserialize a hello message from a message frame object.
        data format is (length, string, length, string)
        """
        uint32_t_size = struct.calcsize("!I")
        job_name_bytes = struct.unpack_from("!I", msgframe.data, 0)[0]
        job_name = struct.unpack_from(
            str(job_name_bytes) + "s",
            msgframe.data,
            uint32_t_size
        )[0]
        host_name_bytes = struct.unpack_from(
            "!I",
            msgframe.data,
            uint32_t_size + job_name_bytes
        )[0]
        host_name = struct.unpack_from(
            str(host_name_bytes) + "s",
            msgframe.data,
            2*uint32_t_size + job_name_bytes
        )[0]
        return cls(host_name.decode('utf-8'), job_name.decode('utf-8'))
