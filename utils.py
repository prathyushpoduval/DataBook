import socket
import json
import numpy as np


def check_port( port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect(('localhost', port))
            return True
        except ConnectionRefusedError:
            return False
        
def send_message(port, message,response_req=False):

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', port))
            s.sendall(message.encode('utf-8'))

            if response_req:
                response = s.recv(102400)
                return response
            
    except ConnectionRefusedError:
        print(f"Failed to connect to node on port {port}. Make sure the node server is running.")



def send_transaction(node_id, transactions,response_req=False):
    message = json.dumps(transactions)
    return send_message(node_id, message,response_req=response_req)
 


            
def compress_hops_nodes(hops,nodes):
    node_unique_list=[]
    hop_collated=[]
    for n in nodes:
        if n not in node_unique_list:
            node_unique_list.append(n)
            hop_collated.append(hops[nodes==n])

    return hop_collated,node_unique_list


def perform_first_hop(hops,node,send=True):

    if send:
        first_hop=send_transaction(node[0],[hops[0]],True)
        #json object
        first_hop=json.loads(first_hop)
        first_hop=first_hop[0]
        
        hops=np.array(hops[1:])
        node=np.array(node[1:])
        hops,node=compress_hops_nodes(hops,node)

    else:
        first_hop="-1"
        
        hops=np.array(hops[1:])
        node=np.array(node[1:])
        hops,node=compress_hops_nodes(hops,node)


    return first_hop,hops,node
