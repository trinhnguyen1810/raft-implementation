# Simple Raft Consensus Implementation

A Python implementation of the Raft consensus algorithm for distributed systems. This implementation includes a node manager for monitoring and controlling the Raft cluster.

## Overview

The system consists of:
- 3 pre-defined Raft nodes that can act as Leader, Follower, or Candidate, with those instances being able to communicate to each other
- Leader Election Process that aligns with the Raft Protocol
- A Node Manager for monitoring and controlling the cluster
based on a ZMQ-based network communication layer

## Setup

### Prerequisites
- Python 3.7+
- virtualenv (recommended)

### Installation

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Key Dependencies
- Python 3.7+
- pyzmq
- threading
- json

## Configuration

Create a `config.json` file in the `src` directory if not already found. It should be in the directory already:
```json
{
    "1": {"ip": "127.0.0.1", "port": "5001"},
    "2": {"ip": "127.0.0.1", "port": "5002"},
    "3": {"ip": "127.0.0.1", "port": "5003"}
}
```

## Running the System

### 1. Start Node Manager

First, start the node manager which coordinates communication between nodes:

```bash
cd src/raft_implementation
python node_manager.py config.json
```

The node manager provides a CLI interface with the following commands:
- `status` - Show status of all nodes
- `set <id> <state>` - Set node state (FOLLOWER/CANDIDATE/LEADER)
- `help` - Show help message
- `quit` - Exit the program

### 2. Start Raft Nodes

Open separate terminals for each node and activate the virtual environment in each:

```bash
# Terminal 1
source venv/bin/activate  # On Windows: venv\Scripts\activate
cd src/raft_implementation
python run_node.py 1

# Terminal 2
source venv/bin/activate  # On Windows: venv\Scripts\activate
cd src/raft_implementation
python run_node.py 2

# Terminal 3
source venv/bin/activate  # On Windows: venv\Scripts\activate
cd src/raft_implementation
python run_node.py 3
```
As you run each node, use `status` command to see the status. Alternatively, there should logging statements (which may be decluttering as I mainly used to debug)

## Testing Fault Tolerance

### Leader Failure Test
1. Use `status` command in node manager to identify current leader
2. Kill the leader node (Ctrl+C)
3. Watch remaining nodes elect new leader
4. Use `status` command to verify new leader

### Network Partition Test
1. Start all nodes
2. Kill a follower node
3. Verify system continues functioning
4. Restart node to test rejoin behavior

## Monitoring

### Node States
Nodes can be in one of three states:
- FOLLOWER
- CANDIDATE
- LEADER

### Checking Status
Use the node manager's `status` command to see:
- Current state of each node
- Term numbers
- Node activity status
- Last heartbeat times
- Current leader

Example output:
```
Node Status:
--------------------------------------------------
Node ID | State    | Term | Status  | Last Seen
--------------------------------------------------
1       | LEADER   | 2    | ACTIVE  | 1s ago
2       | FOLLOWER | 2    | ACTIVE  | 2s ago
3       | FOLLOWER | 2    | ACTIVE  | 1s ago
--------------------------------------------------
Current Leader: 127.0.0.1:5001
```

## Project Structure
```
src/
└── raft_implementation/
    ├── config.json
    ├── node_manager.py
    ├── run_node.py
    └── requirements.txt
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
# raft-implementation
