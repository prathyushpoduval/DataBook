import socket
import sqlite3
import json
from utils import *

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

def start_node(node_id, port,main_port):

    # Initialize the database for this node
    database_name = f"node_{node_id}.db"
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()
    create_tables(conn)


    print(f"Node {node_id} recieved main port {main_port}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen()

    print(f"Node {node_id} started on port {port}")

    while True:
        conn_main, addr = server.accept()
        with conn_main:
            data = conn_main.recv(1024)
            hops=json.loads(data)

            response_list=[]
            while True:
                try:
                    response_list=[]
                    for hop in hops:
                        cursor.execute(hop)
                        response_list.append(cursor.fetchall())

                    conn.commit()
                    break
                except sqlite3.OperationalError:
                    print(f"Database locked. Trying again. Node at Port {node_id}")
                    continue
            print(f"Node {node_id} finished processing transaction, with response\n {response_list}")
            response_list=json.dumps(response_list)
            #print(main_port)
            send_message(main_port,response_list)

