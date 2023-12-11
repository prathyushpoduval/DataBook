from multiprocessing import Process
import node
import socket
import time

def run_node(node_id, port):
    node.start_node(node_id, port)

def send_message(port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', port))
            s.sendall(message.encode('utf-8'))
            response = s.recv(1024)
            print(f"Response from node: {response.decode('utf-8')}")
    except ConnectionRefusedError:
        print(f"Failed to connect to node on port {port}. Make sure the node server is running.")


def main():
    nodes = [2000, 2001, 2002]
    processes = []

    for i, port in enumerate(nodes):
        p = Process(target=run_node, args=(i, port))
        p.start()
        processes.append(p)

    # Print PIDs of all started processes
    for i, process in enumerate(processes):
        print(f"Node {i} started with PID: {process.pid}")


if __name__ == "__main__":
    
    main()
    
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
