import socket
import socketserver
import threading
from socketserver import ThreadingMixIn, TCPServer, BaseRequestHandler
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext
from MessageFrame import *
import shlex
import select
import os
from collections import namedtuple

subscriber_t = namedtuple('subscriber_t', ('user_id', 'chat_id', 'events'))
job_t = namedtuple('job_t', ('host_name', 'job_name'))



class JobUpdateDB:
    """
    map of workers -> status updates
    map of workers -> who is subscribed
    list of subscribers & interest lists
    """


    def __init__(self):
        self._subscribers = list()
        self._jobs_on_host = dict()


    @property
    def hosts(self):
        return self._jobs_on_host.keys()


    def active_jobs(self, on_host):
        ...


    def exited_jobs(self, on_host):
        ...


class ThreadingLocalServer(ThreadingMixIn, TCPServer):
    ...
    # def handle_error(self, request, client_address):
        # ...

class TaskUpdateRequestHandler(BaseRequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tg_bot = None


    @property
    def tg_bot(self):
        return self._tg_bot


    @tg_bot.setter
    def tg_bot(self, val):
        self._tg_bot = val
    

    def handle(self):
        # this function is only called once per connection. who knew?!
        # not the documentation!
        socket = self.request
        socket.setblocking(False)
        fd = socket.fileno()
        ep = select.epoll(1)
        ep.register(fd, select.EPOLLIN | select.EPOLLET | select.EPOLLEXCLUSIVE)
        while True:
            events = ep.poll(-1)
            while True:
                header = socket.recv(MessageFrame.header_len)
                read_len = MessageFrame.get_message_size(header)
                if read_len < 0:
                    # incomplete message header... try again and hope we don't get
                    # out of sync
                    break

                msg = MessageFrame.unpack_data(header, socket.recv(read_len))
                if msg is None:
                    epoll.close()
                    raise RuntimeError(f"Got an incomplete message from {socket.getpeername()}. Bailing out!")

                self.decode_and_process(msg)


    def decode_and_process(self, msg):
        if msg.type == MessageType.HELLO.value:
            hmsg = HelloMessage.from_msg_frame(msg)
            print(f'got HELLO from {hmsg.host_name}:{hmsg.job_name}')
            # global bot
            # global chat
            # bot.send_message(chat, text=f'got HELLO from {hmsg.host_name}:{hmsg.job_name}')

        if msg.type == MessageType.GOODBYE.value:
            gmsg = GoodbyeMessage.from_msg_frame(msg)
            if gmsg.job_name is not None:
                print(f'got GOODBYE from {gmsg.host_name}:{gmsg.job_name}')
            else:
                print(f'got GOODBYE from {gmsg.host_name}')


bot, chat = None, None


def start_fn(update: Update, context: CallbackContext):
    chat_id = update.message.chat_id
    update.message.reply_text("it's been a very long time...")


def subscribe_fn(update: Update, context: CallbackContext):
    msg = update.message
    chat_id = msg.chat_id
    args = shlex.split(msg.text)
    host = args[1]
    job = args[2]

    update.message.reply_text(f"Got it. I'll subscribe you to updates from {host} about {job}.")
    global bot
    global chat
    bot = context.bot
    chat = chat_id


def main():
    # get token
    with open('token.dat', 'r') as f:
        BOT_TOKEN = f.readline().strip()


    # job_map = RemoteJobMap()
    
    tg_updater = Updater(BOT_TOKEN)
    tg_dispatcher = tg_updater.dispatcher

    tg_dispatcher.add_handler(CommandHandler("start", start_fn))
    tg_dispatcher.add_handler(CommandHandler("subscribe", subscribe_fn))

    tg_job_queue = tg_updater.start_polling()

    tcp_server = ThreadingLocalServer(("0.0.0.0", 8580), TaskUpdateRequestHandler)
    tcp_server.tg_bot = tg_updater.bot
    tcp_server_thread = threading.Thread(target=tcp_server.serve_forever)
    tcp_server_thread.daemon = True
    tcp_server_thread.start()

    # tg_updater.idle()
    tcp_server.serve_forever()

    tcp_server.server_close()
    tcp_server.shutdown()


if __name__ == '__main__':
    main()
