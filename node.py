import socket
import json

def start_node(node_id, port, other_nodes):
    # Start the server to listen to other nodes
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', port))
    server.listen()

    # Database interaction code goes here

    # Example client to send a message to another node
    def send_message(target_port, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', target_port))
            s.sendall(json.dumps(message).encode('utf-8'))

    # Main loop for the server
    while True:
        conn, addr = server.accept()
        with conn:
            data = conn.recv(1024)
            if not data:
                break
            # Handle received data
            print(f"Node {node_id} received: {data.decode('utf-8')}")
