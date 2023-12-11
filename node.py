import socket
import threading

def start_node(node_id, port):

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
                print(f"Node {node_id} received: {message}")
                conn.sendall(f"Node {node_id} got your message: '{message}'".encode('utf-8'))
