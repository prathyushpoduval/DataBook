from multiprocessing import Process
import node
import socket
import time
import transactions_sql as trans
import numpy as np
import json
from utils import *

NUM_NODE=3



def run_node(node_id, port,main_port):
    node.start_node(node_id, port,main_port)

def main():

    main_port=2900

    node_ports= [2100+i for i in range(NUM_NODE)]
    
    processes = []

    for i, port in enumerate(node_ports):
        p = Process(target=run_node, args=(i, port,main_port))
        p.start()
        processes.append(p)

    time.sleep(2)


    # Print PIDs of all started processes
    for i, process in enumerate(processes):
        print(f"Process {i} started with PID: {process.pid}")

   
    
    # ASKS FOR TRANSACTIONS
    #FILL 
    user_node_dict={}

    #Get User dict
    for i in range(NUM_NODE):
        transaction=f"SELECT user_id FROM Users"
        resp=send_transaction(node_ports[i],[transaction],True)
        resp=json.loads(resp)
        print(resp)
        resp=resp[0]
        for j in resp:
            user_node_dict[j[0]]=i

    print("User Node dict loaded....")
        

    #COMPLETE

    ######

    tranID=0
    while True:
        hops = None
        user_node = None
        
        transaction = input("Enter transaction and parameters (or 'exit'): ")
        if transaction == "exit":
            break

        tranID+=1

        response_req=False
        
        # create user
        if transaction == "create_user":
            user_id=input("Enter user_id: ")
            user_name=input("Enter user_name: ")
            user_node=int(input("Enter user_node: "))

            user_node_dict[user_id]=user_node
            timestamp=time.time()
            
            
            hops,node=trans.create_user(user_id,user_name,timestamp)
            print(node)
            print(user_node_dict)
            node=[node_ports[user_node_dict[i]] for i in node]

            

            resp,hops,node=perform_first_hop(hops,node)

            if len(resp)!=0:
                print("User already exists, Aborted")
                continue
            else:
                print("User created")
            
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")

        
        # create friendship
        elif transaction == "create_friendship":
            user_id1=input("Enter user_id1: ")
            user_id2=input("Enter user_id2: ")

            timestamp=time.time()

            hops,node=trans.create_friendship(user_id1,user_id2,timestamp)

            node=[node_ports[user_node_dict[i]] for i in node]

            resp,hops,node=perform_first_hop(hops,node)

            if len(resp)!=0:
                print("Friendship already exists, Aborted")
                continue
            else:
                print("Friendship created")
            
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")
        
        # create post
        elif transaction == "create_post":
            post_id=input("Enter post_id: ")
            user_id=input("Enter user_id: ")
            content=input("Enter content: ")

            timestamp=time.time()
            
            hops,node=trans.create_post(post_id,user_id, timestamp, content)
            
            node=[node_ports[user_node_dict[i]] for i in node]

            resp,hops,node=perform_first_hop(hops,node)

            if len(resp)!=0:
                print("Post_id already exists, Aborted")
                continue
            else:
                print("Post created")
        
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")
            
        # like post
        elif transaction == "like_post":
            user_id=input("Enter user_id: ")
            post_user_id=input("Enter user_id_of_post: ")
            post_id=input("Enter post_id: ")

            timestamp=time.time()
            
            hops,node=trans.like_post(user_id,post_user_id, post_id, timestamp)

            node=[node_ports[user_node_dict[i]] for i in node]
            
            resp,hops,node=perform_first_hop(hops,node)

            if len(resp)==0:
                print("Post_id does not exist, Aborted")
                continue
            else:
                print("Like created")
                
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")
            
        # edit post
        elif transaction == "edit_post":
            user_id=input("Enter user_id: ")
            post_id=input("Enter post_id: ")
            user_node=int(input("Enter user_node: "))

            user_node_dict[user_id]=user_node
            content=input("Enter content: ")
            
            timestamp=time.time()
            
            hops,node=trans.edit_post(user_id, post_id, content)

            node=[node_ports[user_node_dict[i]] for i in node]
            
            resp,hops,node=perform_first_hop(hops,node)

            if len(resp)==0:
                print("Post_id does not exist, Aborted")
                continue
            else:
                print("Post edited")
                
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")
        
        # timeline query 
        # THIS DOES NOT MATCH THE TRANSACTION DEFINITION
        elif transaction == "timeline":
            user_id=input("Enter user_id: ")
            node=input("Enter node: ")
            #uncertain about node
            
            timestamp=time.time()
            
            hops=trans.timeline_query(user_id, node)


            resp,hops,node=perform_first_hop(hops,[user_node_dict[user_id],0])

            friendlist=[node_ports[user_node_dict[i[0]]] for i in resp]
            hoplist=[hops[-1] for i in resp]

            hops,node=compress_hops_nodes(hoplist,friendlist)

            response_req=True
            
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")
            
        elif transaction == "print_all_tables":
            hops=trans.print_all_tables()
            node=list(range(NUM_NODE))
            node=[node_ports[i] for i in node]


            hop_list=[hops for i in range(NUM_NODE)]
            hops=hop_list

            response_req=True
        
        elif transaction == "remove_user":
            user_id=input("Enter user_id: ")
            user_node=int(input("Enter user_node: "))

            timestamp = time.time()
            
            user_node_dict[user_id]=user_node     
    
            hops,node=trans.remove_user(user_id)
            node=[node_ports[user_node_dict[i]] for i in node]

            
            resp,hops,node=perform_first_hop(hops,node)

            if len(resp)!=0:
                print("User does not exist, not deleted")
                continue
            else:
                print("User deleted")
                
            latency = time.time()-timestamp
            print("Latency: " + str(latency) + " ms")
                
        else:
            print("Invalid transaction.")
            continue
        
        ########  

        for h,n in zip(hops,node):
            # Objects of type ndarray are not JSON serializable
            h = list(h)
            if response_req:
                resp=send_transaction(n,h,True)
                resp=json.loads(resp)
                print(resp)
            else:
                send_transaction(n,h)
            print(f"Hop Executed on Node {n}:\n")

            
    return node_ports,processes


if __name__ == "__main__":
    
    nodes, processes=main()

    # Increase this time if the nodes are not ready
    time.sleep(2)

    try:
        while True:
            node_number = int(input("Enter node number (0, 1, 2): "))
            message = input("Enter message: ")
            if node_number in range(len(nodes)):
                send_message(nodes[node_number], message)
            else:
                print("Invalid node number.")
    except KeyboardInterrupt:
        print("\nExiting...")

    for process in processes:
        process.terminate()
