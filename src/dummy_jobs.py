import socket
import sys
from MessageFrame import *
from time import sleep
HOST, PORT = "localhost", 8580

def dummy_job(sock):
    while True:
        sock.sendall(MessageFrame.compose(StatusChangeMessage()))


with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))
    sock.sendall(MessageFrame.compose(HelloMessage("localhost", "dummy")))
    dummy_job(sock)
