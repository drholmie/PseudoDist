#$ curl -v -i http://127.0.0.1:8000/GET?key=abc
import time
from http.server import BaseHTTPRequestHandler
from urllib import parse
import os
import json
import simplejson #for reading put data as json object
import socket
import requests

class handleRequests(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        
    def do_GET(self):
        self._set_headers()
        parsed_path = parse.urlparse(self.path)
        query = parsed_path.query

        if 'search' in query:   ##get request for particular storage node
            search = query[query.find('search')+7:]
            if(findStorageNode(search) or f==2):
                self.wfile.write(bytes(ipaddr, 'utf-8'))
                return
            else:
                self.wfile.write(bytes("not found", 'utf-8'))
                return
            
        elif 'file' in query:
            with open('bkpdata.txt', 'r+') as json_file:
                sendbkpdata = json.load(json_file)
                self.wfile.write(bytes(json.dumps(sendbkpdata),'utf-8'))
                return

        elif 'bkp' in query:
            with open('data.txt', 'r+') as json_file:
                sendbkpdata = json.load(json_file)
                self.wfile.write(bytes(json.dumps(sendbkpdata),'utf-8'))
                return

        key = query[query.find('key')+4:]

        values = []
        if(f==1):
            file = 'bkpdata.txt'
        else:
            file = 'data.txt'
        with open(file, 'r') as json_file:
            data = json.load(json_file)
            try:
                values = data[key]
                #print(values)
                values = str(values)
                self.wfile.write(bytes(values, 'utf-8'))
            except KeyError:
                error = "Key not found!"
                self.wfile.write(bytes(error, 'utf-8'))

    def do_PUT(self):
        self._set_headers()
        content_length = int(self.headers['Content-Length'])
        put_data = self.rfile.read(content_length)

        data = simplejson.loads(put_data)

        key = data['key']
        print("key is", key)
        values = data['values']
        print("values is", values)
        
        with open("conf.txt", "r") as f:
            range_val = f.readline()
            first=range_val.split("-")[0]
            last=range_val.split("-")[1]
            last=last.splitlines()[0]
            bkp_range_val = f.readline()
            bkp_first=bkp_range_val.split("-")[0]
            bkp_last=bkp_range_val.split("-")[1]
            bkp_last=bkp_last.splitlines()[0]
            #add if condition!!!
            if first<=key<=last:
                with open('data.txt', 'r+') as json_file:
                    kvstore_data = json.load(json_file)
                    print(type(kvstore_data))
                    if key in kvstore_data.keys():
                        error = "Key already present! Try with a different key."
                        self.wfile.write(bytes(error, 'utf-8'))
                        return
                    else:
                        kvstore_data[key] = values
                        json_file.seek(0)
                        json.dump(kvstore_data, json_file)
                        message = "SUCCESFULLY WRITTEN"
                        self.wfile.write(bytes(message, 'utf-8'))
                        return
            elif bkp_first<=key<=bkp_last:
                with open('bkpdata.txt', 'r+') as json_file:
                    kvstore_data = json.load(json_file)
                    print(type(kvstore_data))
                    if key in kvstore_data.keys():
                        error = "Key already present! Try with a different key."
                        self.wfile.write(bytes(error, 'utf-8'))
                
                    else:
                        kvstore_data[key] = values
                        json_file.seek(0)
                        json.dump(kvstore_data, json_file)
                        message = "SUCCESFULLY WRITTEN"
                        self.wfile.write(bytes(message, 'utf-8'))
def rval():
    with open("conf.txt", "r") as f:
            range_val = f.read(3)
            return range_val
def sync():
    first=""
    flag=0
    with open("conf.txt", "r") as f:
        range_val = f.read(3)
        first=range_val.split("-")[0]
        last=range_val.split("-")[1]
    children=zk.get_children("/zkslave")
    payload = {'search':first}
    for child in children:
         data, stat=zk.get("/zkslave/"+child)
         ip=data.decode("utf-8").split("+")[0]
         if(ip==str(ipaddr)):
             continue
         r = requests.get('http://'+ip+':8000/get', params=payload)
         print(r.text)
         if r.text != 'not found':
              flag=1
              break
    if(flag==0):
      print("sync not required")
      return
    ip=r.text
    payload = {'file':first}
    r = requests.get('http://'+ip+':8000/get',params=payload)
    recvbkpdata = r.json()
    print(recvbkpdata)
    with open("data.txt", "r+") as json_file:
        json_file.seek(0)
        if len((json.load(json_file)).keys())==len(recvbkpdata.keys()):
            return
    with open("data.txt", "w+") as json_file:
        json.dump(recvbkpdata, json_file)
    #send entire json object for new function sendfile
        

def bkpsync():
    first=""
    flag=0
    with open("conf.txt", "r") as f:
        f.readline()
        range_val = f.read(3)
        first=range_val.split("-")[0]
        last=range_val.split("-")[1]
    children=zk.get_children("/zkslave")
    payload = {'search':first}
    for child in children:
         data, stat=zk.get("/zkslave/"+child)
         ip=data.decode("utf-8").split("+")[0]
         if(ip==str(ipaddr)):
             continue
         r = requests.get('http://'+ip+':8000/get', params=payload)
         print(r.text)
         if r.text != 'not found':
              flag=1
              break
    if(flag==0):
      print("bkpsync not required")
      return
    ip=r.text
    payload = {'bkp':first}
    r = requests.get('http://'+ip+':8000/get',params=payload)
    recvbkpdata = r.json()
    print(recvbkpdata)
    with open("bkpdata.txt", "r+") as json_file:
        json_file.seek(0)
        if len((json.load(json_file)).keys())==len(recvbkpdata.keys()):
            return 
    with open("bkpdata.txt", "w+") as json_file:
        json.dump(recvbkpdata, json_file)

#load and send json object from file
    
def init():
    global node
    temp=str(ipaddr)+"+"+rval()
    if not zk.exists("/zkslave"):
        zk.create("/zkslave")
    if zk.exists("/master"):
        zk.delete(node)
        node=zk.create("/zkslave/slave",bytes(temp, 'utf-8'),ephemeral=True, sequence=True)
    else:
        l=zk.get_children("/")
        l.sort()
        if node=="/"+l[0]:
            zk.delete(node)
            node=zk.create("/master",bytes(temp,'utf-8'), ephemeral=True)
            zk.create("/zkslave/master",bytes(temp,'utf-8'), ephemeral=True)
        else:
            time.sleep(3)
            if node=="/"+l[1]:
                zk.delete(node)
                node=zk.create("/sub-master",bytes(temp,'utf-8'),ephemeral=True)
            else:
                zk.delete(node)
                node=zk.create("/zkslave/slave",bytes(temp,'utf-8'),ephemeral=True, sequence=True)
    sync()
    bkpsync() 

    
def update(cnode):
    temp=str(ipaddr)+"+"+rval()
    global node
    node=zk.create(cnode,bytes(temp,'utf-8'),ephemeral=True)
        
def findStorageNode(search):
    global missing
    global f
    with open("conf.txt", "r") as f:
        range_val = f.readline()
        bkp_range_val=f.readline()
        first=range_val.split("-")[0]
        last=range_val.split("-")[1]
        last=last.splitlines()[0]
        bkp_first=bkp_range_val.split("-")[0]
        bkp_last=bkp_range_val.split("-")[1]
        bkp_last=bkp_last.splitlines()[0]
        if first<=search<=last:
           return True
        elif bkp_first<=search<=bkp_last:
           f=2
           for x in missing:
             mfirst=x.split("-")[0]
             mlast=x.split("-")[1]
             print("Found in backup")
             if mfirst<=search<=mlast:
                print("Returned by backup")
                f=1
                return True
             else:
                return False           
        return False
    
        
if __name__ == '__main__':
    from http.server import HTTPServer
    from kazoo.client import KazooClient

    #---------------------------------------------------------
    f=0
    s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    s.connect(("192.168.43.228",80))
    ipaddr=s.getsockname()[0]             
    zk = KazooClient(hosts='192.168.43.228:2181')
    #zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    active=[]
    missing=[]
    node=zk.create("/zknode",b"junkdata",ephemeral=True,sequence=True)
    init()
    @zk.ChildrenWatch("/")
    def lead(children):
       global node
       time.sleep(5)
       s=zk.get_children("/zkslave")
       if len(s)>0 and not (zk.exists("/master")):
        print (node)
        print(s)
        s.sort()
        if node=="/zkslave/"+s[0]:
         zk.delete(node)
         node="/master"
         update("/master")
         update("/zkslave/master")
         print("master died, new master created")
    @zk.ChildrenWatch("/zkslave")
    def slav(children):
        global node
        global active
        global missing
        if len(children)==0:
            print("no slaves online")
            return
        children=list(map(lambda x: zk.get("/zkslave/"+x)[0].decode('utf-8'),children))
        children=list(map(lambda x: x.split("+")[1],children))
        if len(children)>len(active):
            active=children
            print("server online")
            return
        elif len(children)<len(active):
            missing=list(set(active)-set(children))
            ser(missing)
            return
    def ser(missing):
         for x in missing:
             mfirst=x.split("-")[0]
             mlast=x.split("-")[1]
             with open("conf.txt", "r") as f:
                 r_val = f.readline()
                 range_val=f.readline()
                 first=range_val.split("-")[0]
                 last=range_val.split("-")[1]
                 last=last.splitlines()[0]
                 if mfirst==first and mlast==last:
                     print("Server for "+mfirst+"-"+ mlast+" went down:Backup found")
    
                 else:
                     print("Server for "+mfirst+"-"+ mlast+" went down, Checking other nodes")    
    #---------------------------------------------------------
    host = ipaddr
    #host="127.0.0.1"
    port = 8000
    #port=8100
    server = HTTPServer((host, port), handleRequests)
    server.serve_forever()
