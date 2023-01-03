import socket, errno
import time
import logging
import shutil
f = open("./server1/log.log",'w')
f.truncate(0)
f.close()
f = open("./server2/log.log",'w')
f.truncate(0)
f.close()
f = open("./server3/log.log",'w')
f.truncate(0)
f.close()


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
i = 0
def check_status(port,i):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", port))
    except socket.error as e:
        
        if e.errno == errno.EADDRINUSE:
            print("Port is already in use ",i)
            return True
        else:
    
            print(e)
    return False

s.close()
n = 0



while True :
    i = i +1
    if(check_status(3125,i)==True):
        f = open("./server1/log.log",'a')
        
         
        s = "Broker 1 is the leader"
      
        f.write(s+'\n')
        f.close()
       
        shutil.copy("./server1/log.log", "./server2/log.log")
        shutil.copy("./server1/log.log", "./server3/log.log")

        n =0
        f = open("global.txt",'w')
        f.write('3125')
        f.close()
        # logging.warning('Broker 1 is the Leader')

        time.sleep(2)

        
        continue
    elif (check_status(3126,i)==True):
       
        f = open("./server2/log.log",'a')
        s = "Broker 2 is the leader"
        
        f.write(s+'\n')
        f.close()

        shutil.copy("./server2/log.log", "./server1/log.log")
        shutil.copy("./server2/log.log", "./server3/log.log")
       
        f = open("global.txt",'w')

        f.write('3126')
        f.close()
        time.sleep(2)
        

        continue
    elif (check_status(3127,i)==True):
        
        f = open("./server3/log.log",'a')
        s = "Broker 3 is the leader"
        
        f.write(s+'\n')
        f.close()
        

        shutil.copy("./server3/log.log", "./server1/log.log")
        shutil.copy("./server3/log.log", "./server2/log.log")
        f = open("global.txt",'w')

        f.write('3127')
        f.close()
        

        time.sleep(2)
        continue 
    else :
        logging.warning('All Brokers Have Failed')
        print("All brokers have failed")
        
        break


