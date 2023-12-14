import socket
import json
import numpy as np

def send_message(port, message):

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', port))
            s.sendall(message.encode('utf-8'))
            
    except ConnectionRefusedError:
        print(f"Failed to connect to node on port {port}. Make sure the node server is running.")

def receive_message(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', port))
        s.listen()
        conn, addr = s.accept()
        with conn:
            data = conn.recv(1024)
            if data:
                message = data.decode('utf-8')
                return message

def send_transaction(node_id, transactions, port,response_req=False):
    message = json.dumps(transactions)
    send_message(node_id, message)
    if response_req:
        response = receive_message(port)
        return response


            
def compress_hops_nodes(hops,nodes):
    node_unique_list=[]
    hop_collated=[]
    for n in nodes:
        if n not in node_unique_list:
            node_unique_list.append(n)
            hop_collated.append(hops[nodes==n])

    return hop_collated,node_unique_list


def perform_first_hop(hops,node,port):
    first_hop=send_transaction(node[0],[hops[0]],port,True)
    #json object
    first_hop=json.loads(first_hop)
    first_hop=first_hop[0]
    
    hops=np.array(hops[1:])
    node=np.array(node[1:])
    hops,node=compress_hops_nodes(hops,node)

    return first_hop,hops,node
