import sys
import json
from node import Node
import time
import logging

def main():
    if len(sys.argv) != 2:
        print("Usage: python run_node.py <node_id>")
        print("node_id must be 1, 2, or 3")
        sys.exit(1)

    node_id = sys.argv[1]
    print(f"Starting node {node_id}")
    
    try:
        # Load config
        with open('config.json', 'r') as f:
            config = json.load(f)
            print(f"Loaded config: {config}")
            
        if node_id not in config:
            print(f"Invalid node_id: {node_id}")
            sys.exit(1)

        # Create and start node
        print(f"Creating node {node_id}")
        try:
            node = Node(node_id, config)
            print("Node created successfully")
        except Exception as e:
            print(f"Failed to create node: {e}")
            raise

        print(f"Starting node {node_id} thread")
        try:
            node._thread.start()
            print(f"Node {node_id} thread started")
        except Exception as e:
            print(f"Failed to start node thread: {e}")
            raise
        
        while True:
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        print(f"\nShutting down node {node_id}...")
        node.stop()
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()