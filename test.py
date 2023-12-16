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
import pandas as pd


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


def main(DB_SIZE=100, num_itr=100,transaction_to_run="create_user", debug=False,user_node_dict=None,first_hop_only=False,send=True):

    main_port=2900

    node_ports= [2100+i for i in range(NUM_NODE)]

    processes = []

    for i, port in enumerate(node_ports):

        p = Process(target=run_node, args=(i, port,main_port))
        p.daemon = True
        p.start()
        processes.append(p)
        
    break_while=False
    while not break_while:
        break_while=True
        time.sleep(0.1)
        for port in node_ports:
            if send_message(port,"test")==-1:
                break_while = False

    for port in node_ports:
        send_message(port,"start")

    print("Ports started....")

    # Print PIDs of all started processes
    if debug:
        for i, process in enumerate(processes):
            print(f"Process {i} started with PID: {process.pid}")

   
    
    # ASKS FOR TRANSACTIONS
    #FILL 
    #user_node_dict={}

    #Get User dict
    #for i in range(NUM_NODE):
    #    transaction=f"SELECT user_id FROM Users"
    #    resp=send_transaction(node_ports[i],[transaction],True)
    #    resp=json.loads(resp)
    #    #print(resp)
    #    resp=resp[0]
    #    for j in resp:
    #        user_node_dict[j[0]]=i

    #print("User Node dict loaded....")
        

    #COMPLETE



    ######
    test = True

    # Create 100 dummy users on randomNodes 
    #check if template_{DB_SIZE}_i.db exists, and copy to node_i.db
    #if not, create template_{DB_SIZE}_i.db using dummy


    if test == True and user_node_dict==None:

        user_node_dict={}
        for x in range(DB_SIZE):
            timestamp=time.time()

            curr_node=random.randint(0,NUM_NODE-1)
            user_node_dict[str(x)]=curr_node

            kwargs={"user_id":str(x),"user_name":f"tester_{x}_{curr_node}","user_node":curr_node}

            make_transaction("create_user",user_node_dict,node_ports,**kwargs)
                

        if debug:
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

            make_transaction("create_friendship",user_node_dict,node_ports, **kwargs)

        if debug:
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
        



    test=True 

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
            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)
        
        elif transaction=="create_friendship":
            user1=random.choice(list(user_node_dict.keys()))
            user2=random.choice(list(user_node_dict.keys()))
            while user2==user1:
                user2=random.choice(list(user_node_dict.keys()))

            kwargs={"user_id1":user1,"user_id2":user2}

            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)

        elif transaction=="create_post":
            user_id=random.choice(list(user_node_dict.keys()))
            content=f"latency_{i}_{user_id}"
            post_id=f"post_{i}_{user_id}"
            kwargs={"post_id":post_id,"user_id":user_id,"content":content}
            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)

        elif transaction=="like_post":
            user_id=random.choice(list(user_node_dict.keys()))
            post_user_id=random.choice(list(user_node_dict.keys()))
            while post_user_id==user_id:
                post_user_id=random.choice(list(user_node_dict.keys()))
            post_id=f'post_{i}_{post_user_id}'

            kwargs={"user_id":user_id,"post_user_id":post_user_id,"post_id":post_id}

            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)

        elif transaction=="edit_post":
            user_id=random.choice(list(user_node_dict.keys()))
            post_id=f'post_{i}_{user_id}'
            content=f"edited_latency_{i}_{user_id}"
            kwargs={"user_id":user_id,"post_id":post_id,"content":content}
            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)

        elif transaction=="timeline":
            user_id=random.choice(list(user_node_dict.keys()))
            kwargs={"user_id":user_id}
            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)

        elif transaction=="remove_user":
            user_id=random.choice(list(user_node_dict.keys()))
            kwargs={"user_id":user_id}
            make_transaction(transaction,user_node_dict,node_ports,first_hop_only=first_hop_only,send=send,**kwargs)
            del user_node_dict[user_id]

    time_end=time.time()

    print(f"Time taken for {num_itr} {transaction} transactions: {time_end-time_start} seconds")
    print(f"Average time per transaction: {(time_end-time_start)/num_itr} seconds")




        
            
    return user_node_dict,node_ports,processes,(time_end-time_start)/num_itr


if __name__ == "__main__":
    
    transaction_list=["create_user","create_friendship","create_post","like_post","edit_post","timeline","remove_user"]
    database_length=[100,1000,10000]
    num_itr=50

    


    time_first_hop=np.zeros((len(database_length),len(transaction_list)))
    print("Running first hop transactions\n\n\n\n")
    for db_size in database_length:

        user_node_dict=None

        #delete *.db files
        for i in range(NUM_NODE):
            database_name = f"node_{i}.db"
            try:
                os.remove(database_name)
            except OSError:
                pass
        for transaction in transaction_list:
            print(f"Running {transaction} transactions")
            user_node_dict,node_ports,processes,latency=main(DB_SIZE=db_size, num_itr=num_itr,transaction_to_run=transaction,first_hop_only=True,user_node_dict=user_node_dict)

            time_first_hop[database_length.index(db_size),transaction_list.index(transaction)]=latency

            for process in processes:
                process.terminate()
                process.join()

     
    df=pd.DataFrame(time_first_hop,index=database_length,columns=transaction_list)
    df.to_csv("time_first_hop.csv")

    time_constant=np.zeros((len(database_length),len(transaction_list)))

    print("Running constant transactions\n\n\n\n")
    for db_size in database_length:

        user_node_dict=None

        #delete *.db files
        for i in range(NUM_NODE):
            database_name = f"node_{i}.db"
            try:
                os.remove(database_name)
            except OSError:
                pass
        for transaction in transaction_list:
            print(f"Running {transaction} transactions")
            user_node_dict,node_ports,processes,latency=main(DB_SIZE=db_size, num_itr=num_itr,transaction_to_run=transaction,send=False,user_node_dict=user_node_dict)

            time_constant[database_length.index(db_size),transaction_list.index(transaction)]=latency

            for process in processes:
                process.terminate()
                process.join()
    
    df=pd.DataFrame(time_constant,index=database_length,columns=transaction_list)
    df.to_csv("time_constant.csv")



    time_total_arr=np.zeros((len(database_length),len(transaction_list)))
    print("Running total transactions\n\n\n\n")

    
    for db_size in database_length:

        user_node_dict=None

        #delete *.db files
        for i in range(NUM_NODE):
            database_name = f"node_{i}.db"
            try:
                os.remove(database_name)
            except OSError:
                pass
        for transaction in transaction_list:
            
            print(f"\n\nRunning {transaction} transactions")
            user_node_dict,node_ports,processes,latency=main(DB_SIZE=db_size, num_itr=num_itr,transaction_to_run=transaction,user_node_dict=user_node_dict)

            time_total_arr[database_length.index(db_size),transaction_list.index(transaction)]=latency

            for process in processes:
                process.terminate()
                process.join()

    df=pd.DataFrame(time_total_arr,index=database_length,columns=transaction_list)
    df.to_csv("time_total.csv")