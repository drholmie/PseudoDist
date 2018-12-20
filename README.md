# PseudoDist
A distribute key-value store using zookeeper

# Project Description
The project was to make a distributed key value store using zookeeper. The nodes used were physical systems. One running client code, other's running server code, and one being the zookeeper server. 

The node who contacts zookeeper first becomes master of the cluster. The second fastest becoms the sub master. The rest are just slave nodes. When client contacts zookeeper, it is informed of the status of cluster (master alive or dead). If cluster is alive, zookeeper returns master node's ip address. If cluster is dead returns only status.

High availability is done by duplication and failure tolerance. The data of a node is duplicated across the other nodes. Say node1 stores keys a-h and node 2 stores keys from l-m, node1 acts as node2's backup and vice versa. The backup is stored into a seperate bkpdata file. 
When node dies, or is disconnected, zookeeper triggers the watch function, informing others of a node's death. 

If node was master node, sub master makes itself master, new sub master is also creates, client recieves new master's ip address. 
The backup server now handles requests from client. When failed node comes back online. It syncs its bkpdata with the data of the node who's backup it acts as. For example, node1 dies. Its backup is in node2. When node1 comes back online, it needs to sync its backup data with node2's data, so as to maintain the current status of the cluster. It then proceeds to sync its (node1) data file with node2's bkpdata file. 
