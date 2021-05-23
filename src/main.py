import socket
import socketserver
import threading
from socketserver import ThreadingMixIn, TCPServer, BaseRequestHandler
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
from MessageFrame import *
from io import BytesIO


class RemoteJobMap:
    """
    fancy dictionary
    """


    def __init__(self):
        ...


    @property
    def hosts(self):
        ...


    def active_jobs(self, on_host):
        ...


    def exited_jobs(self, on_host):
        ...


    def request_caching(self, on_host, for_job):
        ...


    def flush_cache(self, on_host, for_job):
        ...



class ThreadingLocalServer(ThreadingMixIn, TCPServer):
    ...
    # def handle_error(self, request, client_address):
        # ...

class TaskUpdateRequestHandler(BaseRequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._job_queue = None
        self.read_buffer = BytesIO(b'')

    @property
    def tg_job_queue(self):
        return self._job_queue

    @tg_job_queue.setter
    def tg_job_queue(self, val):
        self._job_queue = val
    
    def handle(self):
        socket = self.request
        header = socket.recv(MessageFrame.header_len)
        read_len = MessageFrame.get_message_size(header)
        msg = MessageFrame.unpack_data(header, socket.recv(read_len))
        if msg is None:
            raise RuntimeError(f"Got an incomplete message from {socket.getpeername()}. Bailing out!")

        if msg.type == MessageType.HELLO.value:
            hmsg = HelloMessage.from_msg_frame(msg)
            print(f'got HELLO from {hmsg.host_name}:{hmsg.job_name}')


def start_fn(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    update.message.reply_text("it's been a very long time...")

def main():
    # get token
    with open('token.dat', 'r') as f:
        BOT_TOKEN = f.readline().strip()


    job_map = RemoteJobMap()
    
    tg_updater = Updater(BOT_TOKEN)
    tg_dispatcher = tg_updater.dispatcher

    tg_dispatcher.add_handler(CommandHandler("start", start_fn))

    # tg_job_queue = tg_updater.start_polling()

    tcp_server = ThreadingLocalServer(("0.0.0.0", 8580), TaskUpdateRequestHandler)
    tcp_server_thread = threading.Thread(target=tcp_server.serve_forever)
    tcp_server_thread.daemon = True
    tcp_server_thread.start()

    # tg_updater.idle()
    tcp_server.serve_forever()

    tcp_server.server_close()
    tcp_server.shutdown()


if __name__ == '__main__':
    main()
