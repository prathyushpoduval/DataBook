import socket
import sqlite3

def send_message(port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', port))
            s.sendall(message.encode('utf-8'))
            #response = s.recv(1024)
            #print(f"Response from node: {response.decode('utf-8')}")
    except ConnectionRefusedError:
        print(f"Failed to connect to node on port {port}. Make sure the node server is running.")

# Why is there a create tables for the node?
def create_tables(conn):
    cursor = conn.cursor()
    # Define the SQL commands to create tables
    # Example: Create a table named 'example_table'
    cursor.execute('''CREATE TABLE IF NOT EXISTS example_table (
                      id INTEGER PRIMARY KEY,
                      data TEXT)''')
    # Add more table creation commands as needed
    # ...
    conn.commit()

def start_node(node_id, port):

    # Initialize the database for this node
    database_name = f"node_{node_id}.db"
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    create_tables(conn)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen()

    print(f"Node {node_id} started on port {port}")

    
    while True:
        conn, addr = server.accept()

        with conn:
            data = conn.recv(1024)
            if data:
                message = data.decode('utf-8')

                ######
                # execute SQL commands
                
                #cursor.execute(message)

                # abort using rollback() if needed, and send ABORT to main_partner
                # cursor.commit() cursor.rollback()
                #try cursor.execute(message):
                
                    #if ready to commit then:
                        #send COMMIT ready to main

                        #wait for response from main

                        # commit or abort based on main response 
                #catch:
                    #send ABORT to main
                    
                #continue
                ######
                conn.sendall(f"Node {node_id} got your message: '{message}'".encode('utf-8'))

                #COMMIT or ABORT here
