import socket
import fnmatch
import os
import sys
s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
f = open("global1.txt",'r')
s1 = socket.socket()
port1 = int(f.read())
f.close()
f1=open('global1.txt','w')
f1.write(str(port1+1))
f1.close()
port1=port1+1
s1.bind(('localhost',port1))
print ('Socket binded to port '+str(port1))
s1.listen(3)
print ('Consumer is listening to port '+str(port1))


topic_to_search = input("Enter the topic to subscribe : ")
fromBeginning = input("Enter From Beginning flag(1 or 0) : ")
message = str(port1)+","+topic_to_search+","+fromBeginning
path="./topics_customer.txt"
f = open(path, "a")
f.write(message)
f.write('\n')
f.close()
if fromBeginning=='1':
        count1=0
        count_path="./server1/"+topic_to_search+"/partition1"
        count1+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))
        count_path="./server1/"+topic_to_search+"/partition2"
        count1+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))
        count_path="./server1/"+topic_to_search+"/partition3"
        count1+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))
        
        for i in range(count1):
            f = open("global1.txt",'r')
            port1 = int(f.read())
            f.close()
            
            if i%3==0:
                open_path="./server1/"+topic_to_search+"/partition1/"+str(i+1)+".txt"
                f1=open(open_path,'r')
                message_to_consumer1 = f1.readline()
                print(message_to_consumer1)
            elif i%3==1:
                open_path="./server1/"+topic_to_search+"/partition2/"+str(i+1)+".txt"
                f1=open(open_path,'r')
                message_to_consumer1 = f1.readline()
                print(message_to_consumer1)
            elif i%3==2:
                open_path="./server1/"+topic_to_search+"/partition3/"+str(i+1)+".txt"
                f1=open(open_path,'r')
                message_to_consumer1 = f1.readline()
                print(message_to_consumer1)
            
while True:
    c, addr = s1.accept()
    print(str(c.recv(1024).decode()))
    c.close()