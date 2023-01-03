#!/usr/bin/env python
import socket
f = open("global.txt",'r')
s = socket.socket()
port = int(f.read())
f.close()
s.connect(('localhost', port))
topic = input("Enter the topic : ")
message = input("Enter message : ")
z=topic+","+message
# print(z)
s.sendall(z.encode())    
s.close()


