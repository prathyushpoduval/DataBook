from multiprocessing import Process
import node

def run_node(node_id, port, other_nodes):
    node.start_node(node_id, port, other_nodes)

if __name__ == "__main__":
    nodes = [5000, 5001, 5002]  # Port numbers for each node
    processes = []

    for i, port in enumerate(nodes):
        other_nodes = nodes[:i] + nodes[i+1:]
        p = Process(target=run_node, args=(i, port, other_nodes))
        p.start()
        processes.append(p)

    for process in processes:
        process.join()