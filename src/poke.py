import socket
import sys
from MessageFrame import *
HOST, PORT = "localhost", 8580

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.connect((HOST, PORT))
    sock.sendall(MessageFrame.compose(HelloMessage("localhost", "poke")))
