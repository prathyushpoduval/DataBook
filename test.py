from multiprocessing import Process
import node
import socket
import time
import transactions_sql as trans
import numpy as np
import json
from utils import *
import random
import os
import signal
from subprocess import Popen, PIPE


NUM_NODE=3



def run_node(node_id, port,main_port):
    node.start_node(node_id, port,main_port)


def kill_listener(port):

    process = Popen(["lsof", "-i", ":{0}".format(port)], stdout=PIPE, stderr=PIPE)
    stdout, stderr = process.communicate()
    for process in str(stdout.decode("utf-8")).split("\n")[1:]:       
        data = [x for x in process.split(" ") if x != '']
        if (len(data) <= 1):
            continue

        os.kill(int(data[1]), signal.SIGKILL)


def make_transaction(transaction, user_node_dict,node_ports,first_hop_only=False,send=True, **kwargs):
    # create user
    
    response_req=False
    if transaction == "create_user":
        user_id=kwargs["user_id"]
        user_name=kwargs["user_name"]
        user_node=kwargs["user_node"]

        user_node_dict[user_id]=user_node
        timestamp=time.time()
        
        
        hops,node=trans.create_user(user_id,user_name,timestamp)
        #print(node)
        #print(user_node_dict)
        node=[node_ports[user_node_dict[i]] for i in node]

        

        resp,hops,node=perform_first_hop(hops,node,send=send)

        if len(resp)!=0:
            return -1
        
        

    
    # create friendship
    elif transaction == "create_friendship":
        user_id1=kwargs["user_id1"]
        user_id2=kwargs["user_id2"]

        timestamp=time.time()

        hops,node=trans.create_friendship(user_id1,user_id2,timestamp)

        node=[node_ports[user_node_dict[i]] for i in node]

        resp,hops,node=perform_first_hop(hops,node,send=send)

        if len(resp)!=0:
            return -1
    
    # create post
    elif transaction == "create_post":
        post_id=kwargs["post_id"]
        user_id=kwargs["user_id"]
        content=kwargs["content"]

        timestamp=time.time()
        
        hops,node=trans.create_post(post_id,user_id, timestamp, content)
        
        node=[node_ports[user_node_dict[i]] for i in node]

        resp,hops,node=perform_first_hop(hops,node,send=send)

        if len(resp)!=0:
            return -1

    
        
    # like post
    elif transaction == "like_post":

        user_id=kwargs["user_id"]
        post_user_id=kwargs["post_user_id"]
        post_id=kwargs["post_id"]


        timestamp=time.time()
        
        hops,node=trans.like_post(user_id,post_user_id, post_id, timestamp)

        node=[node_ports[user_node_dict[i]] for i in node]
        
        resp,hops,node=perform_first_hop(hops,node,send=send)
        if len(resp)==0:
            return -1

            
        
    # edit post
    elif transaction == "edit_post":

        user_id=kwargs["user_id"]
        post_id=kwargs["post_id"]
        content=kwargs["content"]

        
        timestamp=time.time()
        
        hops,node=trans.edit_post(user_id, post_id, content)

        node=[node_ports[user_node_dict[i]] for i in node]
        
        resp,hops,node=perform_first_hop(hops,node,send=send)

        if len(resp)==0:
            return -1
      
            
    
    # timeline query 
    # THIS DOES NOT MATCH THE TRANSACTION DEFINITION
    elif transaction == "timeline":

        user_id=kwargs["user_id"]
        #uncertain about node
        
        timestamp=time.time()
        
        node=user_node_dict[user_id]
        hops=trans.timeline_query(user_id)


        resp,hops,node=perform_first_hop(hops,[node_ports[user_node_dict[user_id]],0],send=send)

        if send:
            
            friendlist=[node_ports[user_node_dict[i[0]]] for i in resp]

            hop_list=[]
            node_list=[]
            for i in resp:
                user_id_friend=i[0]
                node_friend=user_node_dict[user_id_friend]
                hops_friend=trans.timeline_query(user_id_friend)
                hops_friend=[hops_friend[1]]
                node_list.append(node_ports[node_friend])
                hop_list.append(hops_friend)

            hops,node=compress_hops_nodes(hop_list,node_list)
        response_req=True
        
        
    elif transaction == "print_all_tables":
        hops=trans.print_all_tables()
        node=list(range(NUM_NODE))
        node=[node_ports[i] for i in node]


        hop_list=[hops for i in range(NUM_NODE)]
        hops=hop_list

        response_req=True
    
    elif transaction == "remove_user":

        user_id=kwargs["user_id"]

        timestamp = time.time()
    

        hops,node=trans.remove_user(user_id)
        node=[node_ports[user_node_dict[i]] for i in node]

        
        resp,hops,node=perform_first_hop(hops,node,send=send)


        if not first_hop_only and send:
            for h,n in zip(hops,node):
                h = list(h) 
                send_transaction(n,h)

        #del user_node_dict[user_id]    

        return 1

    else:
        print("Invalid transaction.")
        return -1
    
    ########  

    if not first_hop_only and send:
        for h,n in zip(hops,node):
            # Objects of type ndarray are not JSON serializable
            h = list(h)
            if response_req:
                resp=send_transaction(n,h,True)
                resp=json.loads(resp)
                #print(resp)
            else:
                send_transaction(n,h)
            #print(f"Hop Executed on Node {n}:\n")


def main(DB_SIZE=100, num_itr=100,transaction_to_run="create_user"):

    main_port=2900

    node_ports= [2100+i for i in range(NUM_NODE)]

    processes = []

    for i, port in enumerate(node_ports):

        p = Process(target=run_node, args=(i, port,main_port))
        p.daemon = True
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
        #print(resp)
        resp=resp[0]
        for j in resp:
            user_node_dict[j[0]]=i

    print("User Node dict loaded....")
        

    #COMPLETE



    ######
    test = True

    # Create 100 dummy users on randomNodes 
    if test == True:


        for x in range(DB_SIZE):
            timestamp=time.time()

            curr_node=random.randint(0,NUM_NODE-1)
            user_node_dict[str(x)]=curr_node

            kwargs={"user_id":str(x),"user_name":f"tester_{x}_{curr_node}","user_node":curr_node}

            make_transaction("create_user",user_node_dict,node_ports,**kwargs)
                
        test = False
        print("DUMMIES USERS CREATED")
        print("DUMMY POST CREATED")

        for x in range(DB_SIZE//2):
            #Insert random friendship 
            #Chose user_id1 and 2 randomly from user node dict
            user1=random.choice(list(user_node_dict.keys()))
            user2=random.choice(list(user_node_dict.keys()))
            while user2==user1:
                user2=random.choice(list(user_node_dict.keys()))

            kwargs={"user_id1":user1,"user_id2":user2}

            make_transaction("create_friendship",user_node_dict,node_ports,**kwargs)

        print("DUMMY FRIENDSHIPS CREATED")

        for x in range(DB_SIZE):
            #random likes being inserted
            user_id=random.choice(list(user_node_dict.keys()))
            post_user_id=random.choice(list(user_node_dict.keys()))
            while post_user_id==user_id:
                post_user_id=random.choice(list(user_node_dict.keys()))
            post_id=f'post_0_{post_user_id}'

            kwargs={"user_id":user_id,"post_user_id":post_user_id,"post_id":post_id}

            make_transaction("like_post",user_node_dict,node_ports,**kwargs)

        print("DUMMY LIKES CREATED")
        




    
    current_user_id_count=DB_SIZE+1

    time_start=time.time()

    for i in range(num_itr):
        transaction = transaction_to_run
        
        if transaction=="create_user":
            user_id=str(current_user_id_count)
            current_user_id_count+=1
            node=random.randint(0,NUM_NODE-1)
            user_name=f"latency_{current_user_id_count}_{node}"
            kwargs={"user_id":user_id,"user_name":user_name,"user_node":node}
            make_transaction(transaction,user_node_dict,node_ports,**kwargs)
        
        elif transaction=="create_friendship":
            user1=random.choice(list(user_node_dict.keys()))
            user2=random.choice(list(user_node_dict.keys()))
            while user2==user1:
                user2=random.choice(list(user_node_dict.keys()))

            kwargs={"user_id1":user1,"user_id2":user2}

            make_transaction(transaction,user_node_dict,node_ports,**kwargs)

        elif transaction=="create_post":
            user_id=random.choice(list(user_node_dict.keys()))
            content=f"latency_{i}_{user_id}"
            post_id=f"post_{i}_{user_id}"
            kwargs={"post_id":post_id,"user_id":user_id,"content":content}
            make_transaction(transaction,user_node_dict,node_ports,**kwargs)

        elif transaction=="like_post":
            user_id=random.choice(list(user_node_dict.keys()))
            post_user_id=random.choice(list(user_node_dict.keys()))
            while post_user_id==user_id:
                post_user_id=random.choice(list(user_node_dict.keys()))
            post_id=f'post_{i}_{post_user_id}'

            kwargs={"user_id":user_id,"post_user_id":post_user_id,"post_id":post_id}

            make_transaction(transaction,user_node_dict,node_ports,**kwargs)

        elif transaction=="edit_post":
            user_id=random.choice(list(user_node_dict.keys()))
            post_id=f'post_{i}_{user_id}'
            content=f"edited_latency_{i}_{user_id}"
            kwargs={"user_id":user_id,"post_id":post_id,"content":content}
            make_transaction(transaction,user_node_dict,node_ports,**kwargs)

        elif transaction=="timeline":
            user_id=random.choice(list(user_node_dict.keys()))
            kwargs={"user_id":user_id}
            make_transaction(transaction,user_node_dict,node_ports,**kwargs)

        elif transaction=="remove_user":
            user_id=random.choice(list(user_node_dict.keys()))
            kwargs={"user_id":user_id}
            make_transaction(transaction,user_node_dict,node_ports,**kwargs)
            del user_node_dict[user_id]

    time_end=time.time()

    print(f"Time taken for {num_itr} {transaction} transactions: {time_end-time_start} seconds")
    print(f"Average time per transaction: {(time_end-time_start)/num_itr} seconds")



        
            
    return node_ports,processes


if __name__ == "__main__":
    
    transaction_list=["create_user","create_friendship","create_post","like_post","edit_post","timeline","remove_user"]


    for transaction in transaction_list:
        #delete *.db files
        for i in range(NUM_NODE):
            database_name = f"node_{i}.db"
            try:
                os.remove(database_name)
            except OSError:
                pass
        print(f"Running {transaction} transactions")
        node_ports,processes=main(DB_SIZE=1000, num_itr=500,transaction_to_run=transaction)

        
        
        for process in processes:
            process.terminate()
            process.join()

     
        # Increase this time if the nodes are not ready
        #time.sleep(3)
