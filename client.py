from urllib.parse import urlparse
from kazoo.client import KazooClient
from kazoo.client import KazooState
import time
import sys
import os
import requests
import json


#ZK
def my_listener(state):
    if state == KazooState.LOST:
        print("lost")
    elif state == KazooState.SUSPENDED:
        print("suspended")
    else:
        print("connected")
        
#node data is stored in the format data:node ip addr+range of key values
def masterstat():
    if zk.exists("/master"):
        print("Status :active")
        if len(zk.get_children("/master")) == 0:
            print("No slaves active")
        else:
            print(len(zk.get_children("/master")), "Slaves active")

        data, stat=zk.get("/master")
        print(data.decode("utf-8"), "is the range of values present in master")
         
    else:
        print("node not active")

def onrequest():
    ##client contacts ZK, it returns master addr
    if zk.exists("/master"):
        print("Status:active")
        data, stat=zk.get("/master")
        return data.decode("utf-8").split("+")[0]
    else: 
        print("error: node not active")
        return -1

def contact_master(key, master_addr):
    ##returns storage node addr where key is present
    payload = {'search':key}
    
    r = requests.get('http://'+master_addr+':8000/get', params=payload)
    if r.text != 'not found':
        return master_addr + ':8000'
    
    children=zk.get_children("/zkslave")
    for child in children:
        data, stat=zk.get("/zkslave/"+child)
        ip=data.decode("utf-8").split("+")[0]
        r = requests.get('http://'+ip+':8000/get', params=payload)
        if r.text != 'not found':
            #print("Succesfully written on backup data.")
            break
            
    storage_slave_addr = r.text
    storage_slave_addr = storage_slave_addr + ':8000'
    
    return storage_slave_addr



#client
def input_handler(input_list): #eg for input_list: 'get abc', 'put abc x y z'
    command = input_list[0]
    
    
    if(command == 'get'): # 'get abc'
        key = input_list[1]

        storage_slave_addr = contact_master(key, master_addr)
        #will return addr of storage node where 'key' is present
        print("Storage slave address is: ", storage_slave_addr)
        
        payload = {'key':key}
        r = requests.get('http://'+storage_slave_addr+'/get', params=payload)
        values = r.text
        
        print("Record retrieved is:")
        print(key, values)
        
    elif(command == 'put'): # 'put abc x y z'
        key = input_list[1]
        values = []
        
        for i in range(2, len(input_list)):
            values.append(input_list[i])

        storage_slave_addr = contact_master(key, master_addr)
        print("Storage slave address is: ", storage_slave_addr)
        #will return slave addr where the record will be put
        
        children=zk.get_children("/zkslave")
        data = {'key':key, 'values':values}
        
        for child in children:
            d, stat=zk.get("/zkslave/"+child)
            dat = d.decode("utf-8").split("+")[0]
            ip = dat.split("+")[0]+":8000"
            r = requests.put('http://'+ip+'/put', data=json.dumps(data))
    
        #r = requests.put('http://'+master_addr+':8000/put', data=json.dumps(data))

        print(r.text)
        

    elif(command == 'getmultiple'): #'getmultiple abc def ghi'
        keys = []

        for i in range(1, len(input_list)):
            keys.append(input_list[i])
            
        values_list = [] #list of 'values' for each key

        for key in keys:
            storage_slave_addr = contact_master(key, master_addr)
            print("Storage slave address is: ", storage_slave_addr)
            #values = get(key, storage_slave_addr)
            payload = {'key':key}
            r = requests.get('http://'+storage_slave_addr+'/get', params=payload)
            values = r.text
            values_list.append(values)

        i=0
        print("Records retrieved were:")
        for key in keys:
            print(key, values_list[i])
            i+=1

    elif(command == 'quit' or command == 'exit'):
        exit(0)
        
    else:
        print('ERROR: Command not found.')

#zookeeper intialisation
zk = KazooClient(hosts='192.168.43.228:2181') ##ZK server ip
#zk = KazooClient(hosts='127.0.0.1:2181') ##ZK server ip
zk.start()
zk.add_listener(my_listener)

#main
master_addr = onrequest()#will return the address of the master node, gives -1 on err 
print('Master server addr: ', master_addr)
while(1):
    command_string = input().rstrip()
    input_list = command_string.split(' ')

    input_handler(input_list)
    print("\n\n")
    
