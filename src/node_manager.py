import zmq
import time
import json
import threading
from enum import Enum
from typing import Dict, Optional
from message import MsgType, MsgDirection, StateChangeMsg

class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"

class NodeManager:
    def __init__(self, config_path: str):
        # Load config
        with open(config_path, 'r') as f:
            self.config = json.load(f)
            
        # ZMQ setup
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        
        # Create persistent publisher for state changes
        self.state_publisher = self.context.socket(zmq.PUB)
        self.state_publisher.bind("tcp://*:5555")
        print("Manager state publisher bound to port 5555")
        
        # Connect to all nodes
        for node_id, node_info in self.config.items():
            address = f"tcp://{node_info['ip']}:{node_info['port']}"
            self.socket.connect(address)
            
        # Track node status
        self.node_status: Dict[str, dict] = {}
        self.leader_id: Optional[str] = None
        self.last_heartbeat: Dict[str, float] = {}
        
        # Start monitoring in background thread
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_nodes)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

        # Allow time for connections to establish
        time.sleep(1.0)

    def _monitor_nodes(self):
        """Background thread to monitor node status"""
        poller = zmq.Poller()
        poller.register(self.socket, zmq.POLLIN)
        
        while self.running:
            try:
                events = dict(poller.poll(100))
                if self.socket in events:
                    message = self.socket.recv_json()
                    #print(f"\nNodeManager received message: {message}")  # Debug line
                    
                    if message.get('type') == MsgType.Heartbeat.value:
                        sender = message.get('sender')
                        term = message.get('term')
                        direction = message.get('direction')
                        
                        if direction == MsgDirection.Request.value:
                            self.leader_id = sender
                        
                        self.last_heartbeat[sender] = time.time()
                        self.node_status[sender] = {
                            'term': term,
                            'state': 'LEADER' if sender == self.leader_id else 'FOLLOWER',
                            'last_seen': time.time()
                        }
                    elif message.get('type') == MsgType.Acknowledge.value:
                        #print(f"\nReceived acknowledgment: {message}")
                        sender = message.get('sender')
                        if sender in self.node_status:
                            self.node_status[sender]['state'] = message.get('results', {}).get('new_state', 'UNKNOWN')
                            print(f"Updated node state for {sender} to {self.node_status[sender]['state']}")
                
            except zmq.Again:
                time.sleep(0.1)
            except Exception as e:
                print(f"Monitor error: {e}")

    def set_node_state(self, node_id: str, new_state: str):
        """Set state for a specific node through the Raft leader"""
        try:
            try:
                node_id = int(node_id)
            except ValueError:
                print(f"Error: Node ID must be a number")
                return
                
            # Basic validation
            if str(node_id) not in self.config:
                print(f"Error: Node {node_id} not found")
                return
                
            if not self.leader_id:
                print("Error: No active leader detected. Cannot change state.")
                return

            # Get node info and validate state
            try:
                new_state = NodeState(new_state.upper())
            except ValueError:
                print(f"Error: Invalid state. Must be one of: {[s.value for s in NodeState]}")
                return

            node_addr = f"{self.config[str(node_id)]['ip']}:{self.config[str(node_id)]['port']}"
            current_state = self.node_status.get(node_addr, {}).get('state', 'UNKNOWN')
            cur_term = self.node_status.get(self.leader_id, {}).get('term', 0)

            # Create state change message
            message = {
                'type': MsgType.StateChange.value,
                'term': cur_term,
                'sender': 'manager',
                'receiver': node_addr,
                'direction': MsgDirection.Request.value,
                'new_state': new_state.value
            }

            print(f"\nPreparing to send state change message to {node_addr}")
            print(f"Message content: {message}")
            
            # Send message multiple times to ensure delivery
            for i in range(3):
                print(f"Sending attempt {i+1}...")
                self.state_publisher.send_json(message)
                time.sleep(0.2)  # Increased delay between attempts
            
            print(f"\nRequested state change for Node {node_id}")
            print(f"Current state: {current_state}")
            print(f"Requested state: {new_state.value}")
            print(f"Message sent to: {node_addr}")
                
        except Exception as e:
            print(f"Error setting node state: {e}")

    def show_status(self, timeout: float = 5.0):
        """Display current node status"""
        current_time = time.time()
        print("\nNode Status:")
        print("-" * 50)
        print("Node ID | State    | Term | Status  | Last Seen")
        print("-" * 50)
        
        for node_id, node_info in self.config.items():
            addr = f"{node_info['ip']}:{node_info['port']}"
            node = self.node_status.get(addr, {})
            last_seen = self.last_heartbeat.get(addr, 0)
            
            state = node.get('state', 'UNKNOWN')
            term = node.get('term', '-')
            status = "ACTIVE" if current_time - last_seen < timeout else "INACTIVE"
            last_seen_str = f"{int(current_time - last_seen)}s ago" if last_seen > 0 else "Never"
            
            print(f"{node_id:7} | {state:8} | {term:4} | {status:7} | {last_seen_str}")
        
        print("-" * 50)
        if self.leader_id:
            print(f"Current Leader: {self.leader_id}")
        else:
            print("No leader detected")

    def run_cli(self):
        """Interactive CLI for node management"""
        help_text = """
    Available commands:
        status          - Show status of all nodes
        set <id> <state> - Set node state (FOLLOWER/CANDIDATE/LEADER)
        help            - Show this help message
        quit            - Exit the program
        """
        
        print("Node Manager CLI")
        print("Type 'help' for available commands")
        
        while True:
            try:
                command = input("\n> ").strip().lower()
                parts = command.split()
                
                if not parts:
                    continue
                    
                if parts[0] == "help":
                    print(help_text)
                    
                elif parts[0] == "status":
                    self.show_status()
                    
                elif parts[0] == "set":
                    if len(parts) != 3:
                        print("Usage: set <node_id> <state>")
                        print("States: FOLLOWER, CANDIDATE, LEADER")
                        continue
                        
                    node_id = parts[1]
                    new_state = parts[2].upper()
                    self.set_node_state(node_id, new_state)
                    
                elif parts[0] == "quit":
                    print("Shutting down...")
                    self.close()
                    break
                    
                else:
                    print(f"Unknown command: {parts[0]}")
                    print("Type 'help' for available commands")
                    
            except KeyboardInterrupt:
                print("\nUse 'quit' to exit")
            except Exception as e:
                print(f"Error: {e}")

    def close(self):
        """Cleanup method to close sockets and terminate threads"""
        self.running = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=1.0)
        self.socket.close()
        self.state_publisher.close()  # Close the state publisher
        self.context.term()
        print("NodeManager resources have been cleaned up.")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python node_manager.py <config_file>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    manager = NodeManager(config_path)
    
    try:
        manager.run_cli()
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        manager.close()