import socket
import os
import shutil
import fnmatch
import errno

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
port = 3127
s.bind(('localhost', port))
print ('Socket binded to port',port)
s.listen(3)
print ('socket is listening')

s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
def check_status(port):
    s1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s1.bind(("127.0.0.1", port))
    except socket.error as e:
        
        if e.errno == errno.EADDRINUSE:
            return True
        else:
            return False
    return False

s1.close()

while True:
    count=0
    c, addr = s.accept()
    print ('Got connection from ', addr)
    message_to_consumer=str(c.recv(1024).decode())
    topic,message=message_to_consumer.split(',')
    directory=topic
    f7 = open("./server3/log.log",'a')
    mes = "Message Sent to Broker 3 --The Topic is :: " + str(topic) + " --The Message is :: " + str(message)
    f7.write(mes+'\n')
    
    f7.close()
    isExist=os.path.exists("./server3/"+directory)
    if not isExist:
        parent_dir="./server3"
        path=os.path.join(parent_dir,directory)
        os.mkdir(path)
        parent_dir="./server3/"+topic
        path=os.path.join(parent_dir,"partition1")
        os.mkdir(path)
        parent_dir="./server3/"+topic
        path=os.path.join(parent_dir,"partition2")
        os.mkdir(path)
        parent_dir="./server3/"+topic
        path=os.path.join(parent_dir,"partition3")
        os.mkdir(path)
        
        count_path="./server3/"+topic+"/partition1"
        count+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))

        count_path="./server3/"+topic+"/partition2"
        count+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))


        count_path="./server3/"+topic+"/partition3"
        count+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))

        print(count)
        
        if((count)%3==0):
            file_name = str(count+1)+".txt"
            path="./server3/"+topic+"/partition1/"+file_name
            f = open(path, "a")
            f.write(message+"\n")
            f.close()
        if((count)%3==1):
            file_name = str(count+1)+".txt"
            path="./server3/"+topic+"/partition2/"+file_name
            f = open(path, "a")
            f.write(message+"\n")
            f.close()
        if((count)%3==2):
            file_name = str(count+1)+".txt"
            path="./server3/"+topic+"/partition3/"+file_name
            f = open(path, "a")
            f.write(message+"\n")
            f.close()
    else :
        count_path="./server3/"+topic+"/partition1"
        count+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))

        count_path="./server3/"+topic+"/partition2"
        count+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))


        count_path="./server3/"+topic+"/partition3"
        count+= len(fnmatch.filter(os.listdir(count_path), '*.txt'))
        if((count)%3==0):
            file_name = str(count+1)+".txt"
            path="./server3/"+topic+"/partition1/"+file_name
            f = open(path, "a")
            f.write(message+"\n")
            f.close()
        if((count)%3==1):
            file_name = str(count+1)+".txt"
            path="./server3/"+topic+"/partition2/"+file_name
            f = open(path, "a")
            f.write(message+"\n")
            f.close()
        if((count)%3==2):
            file_name = str(count+1)+".txt"
            path="./server3/"+topic+"/partition3/"+file_name
            f = open(path, "a")
            f.write(message+"\n")
            f.close()
    print(count)       
    source_folder = './server3'
    destination_folder = './server2'
    destination_folder1 = './server1'
    destination_file = './server1/log.txt'
    destination_file1 = './server2/log.txt'

    isExist=os.path.exists(destination_folder)
    isExist1=os.path.exists(destination_folder1)

    if isExist1:
        shutil.rmtree(destination_folder1)
    if isExist:
        shutil.rmtree(destination_folder)


    shutil.copytree(source_folder,destination_folder)
    shutil.copytree(source_folder,destination_folder1)

    #os.remove(destination_file)
    #os.remove(destination_file1)

    f2 = open("topics_customer.txt", "r")
    
    
    for line in f2:
        topic_to_search = line.split(',')[1]
        print(topic_to_search)
        fromBeginning = line.split(',')[2]
        print(fromBeginning)
        port2=int(line.split(',')[0])

        if topic_to_search==topic:
            v1 = socket.socket()
            #port2 = 3129
            #f1 = open("global1.txt",'r')
            #port2 = int(f1.read())
            #print(port2)
            #f1.close()
            #if port is active
            if check_status(port2):
                v1.connect(('localhost', port2))
                v1.sendall(message.encode())  
                v1.close()
                f7 = open("./server1/log.log",'a')
                mes = "Message Sent to Consumer at port "+str(port2)+"--The Topic is :: " + str(topic_to_search) + " --The Message is :: " + str(message)
                f7.write(mes+'\n')
        
                f7.close()

    f2.close() 
    c.close()  
s.close()