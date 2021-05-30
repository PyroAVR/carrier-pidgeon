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
from BidirectionalMap import BidirectionalMap


class ThreadingLocalServer(ThreadingMixIn, TCPServer):
    ...
    # def handle_error(self, request, client_address):
        # ...


class TaskUpdateRequestHandler(BaseRequestHandler):
    """
    Handle data from a connected remote task and enqueue messages to interested
    users when updates have occurred from the connected task.
    """

    def __init__(self, int_list, bot=None, *args, **kwargs):
        """
        int_list: BidriectionalMap, user <-> interest set mapping
        """
        super().__init__(*args, **kwargs)
        self._int_list = int_list
        self._tg_bot = bot


    @property
    def tg_bot(self):
        """
        Instance of a telegram.bot which will be used to send updates.
        """
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
        # TODO better timeout. heartbeat message to prevent killing the thread.
        ep = select.epoll(1)
        ep.register(fd, select.EPOLLIN | select.EPOLLET | select.EPOLLEXCLUSIVE)
        should_continue = True
        while should_continue:
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

                should_continue = self.decode_and_process(msg)


    def decode_and_process(self, msg):
        """
        return: Whether or not we should close the connection. True if we should
        continue listening.
        """
        r = True
        if msg.type == MessageType.HELLO.value:
            hmsg = HelloMessage.from_msg_frame(msg)
            print(f'got HELLO from {hmsg.host_name}:{hmsg.job_name}')
            self._int_list.create_island_b((hmst.host_name, hmsg.job_name))
            # global bot
            # global chat
            # bot.send_message(chat, text=f'got HELLO from {hmsg.host_name}:{hmsg.job_name}')

        elif msg.type == MessageType.GOODBYE.value:
            gmsg = GoodbyeMessage.from_msg_frame(msg)
            if gmsg.job_name is not None:
                print(f'got GOODBYE from {gmsg.host_name}:{gmsg.job_name}')
            else:
                print(f'got GOODBYE from {gmsg.host_name}')


            r = False

        return r


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


    user_int_list = BidirectionalMap()
    
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
