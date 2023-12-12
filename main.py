from multiprocessing import Process
import node
import socket
import time
import transactions_sql as trans

def run_node(node_id, port):
    node.start_node(node_id, port)

def run_node_partner( node_id, port):
    node.start_node_partner(node_id,port)

def send_message(port, message):

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', port))
            s.sendall(message.encode('utf-8'))
            #response = s.recv(1024)
            #print(f"Response from node: {response.decode('utf-8')}")
    except ConnectionRefusedError:
        print(f"Failed to connect to node on port {port}. Make sure the node server is running.")


def main():
    nodes = [2000, 2001, 2002]
    node_partners = [3000, 3001, 3002]

    main_partner_port=4000

    processes = []

    for i, port in enumerate(nodes):
        p = Process(target=run_node, args=(i, port))
        p.start()
        processes.append(p)

    time.sleep(2)
    for i, port in enumerate(node_partners):
        p = Process(target=run_node_partner, args=(i,port))
        p.start()
        processes.append(p)
        time.sleep(2)
        #Send partner port to the node
        send_message(nodes[i], str(port))

    time.sleep(2)
    p=Process(target=main_partner, args=(main_partner_port,nodes, node_partners))
    p.start()
    processes.append(p)

    # Print PIDs of all started processes
    for i, process in enumerate(processes):
        print(f"Process {i} started with PID: {process.pid}")

    # ASKS FOR TRANSACTIONS

    user_node_dict={}

    ######
    #load user_node_list from databases
    #COMPLETE

    ######

    tranID=0
    while True:
        transaction = input("Enter transaction and parameters (or 'exit'): ")
        if transaction == "exit":
            break

        tranID+=1
        
        if transaction == "create_user":
            # Handle create_user transaction
            user_id=input("Enter user_id: ")
            user_name=input("Enter user_name: ")
            user_node=input("Enter user_node: ")
            timestamp=time.time()
            hops,user_node=trans.create_user(user_id,user_name,timestamp)
            
            ########
            #send hops to nodes
            #COMPLETE

    
            #Wait for response from nodes


            # Send COMMIT or ABORT to nodes


            ########

            pass
        elif transaction == "another_transaction":
            # Handle another_transaction
            pass
        else:
            print("Invalid transaction.")
                

    return nodes,processes


def main_partner(port,nodes, node_partners):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen()
    print("Main partner started")


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
