import logging
import time
import random
import threading
from queue import Queue, Empty
import traceback
from typing import Dict, Any, Optional, Set  
from enum import Enum
from interface import MessageSender, MessageReceiver
from message import MsgType, MsgDirection, VoteRequestMsg, LogAppendMsg, VoteResults, LogAppendResults, parse_json_message, Message, StateChangeMsg

class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class Node:
    """Raft consensus node implementation for distributed systems"""

    def __init__(self, node_id: int, config, verbose=True):
        """Initialize a Raft node with configuration
        
        Args:
            node_id: Unique identifier for this node
            config: Either a config dict or path to config file
            verbose: Enable detailed logging if True
        """
        print(f"\n{'='*50}")
        print(f"Starting Node {node_id} Initialization...")
        print(f"{'='*50}")

        # step 1: basic setup
        print("1. Setting up basic configuration...")
        self.node_id = node_id
        self.config = config
        self._init_logging(verbose)
        self._init_threading()

        # step 2: network configuration
        print("2. Configuring network settings...")
        node_info = self._setup_network_config()
        self._init_node_addresses(node_info)
        print(f" → Node Address: {self.current_id}")
        print(f" → Peer Count: {len(self.peer_ids)}")

        # step 3: raft state initialization
        print("3. Initializing Raft state...")
        self._init_raft_state(node_info)
        self._init_log_state()
        #self._init_leader_state(node_info)
        print(f" → Initial State: {self.state.value}")
        print(f" → Current Term: {self.cur_term}")

        # step 4: network components
        print("4. Starting network components...")
        self._init_network_components()
        
        # step 5: timing configuration
        print("5. Setting up timing parameters...")
        self._init_timing_config()

        # completion
        print(f"\nNode {node_id} successfully initialized!")
        print(f"{'='*50}")
        self._log_startup_info()

    def _setup_network_config(self) -> dict:
        """Load and validate network configuration"""
        if isinstance(self.config, dict):
            return self.config
        return self._load_config(self.config, self.node_id)

    def _init_node_addresses(self, node_info: dict):
        """Setup node and peer network addresses"""
        # setup current node address
        self.current_id = f"{node_info[self.node_id]['ip']}:{node_info[self.node_id]['port']}"
        
        # setup peer addresses
        self.peer_ids = [
            f"{node_info[node]['ip']}:{node_info[node]['port']}"
            for node in node_info if node != self.node_id
        ]
        
        # set identity for messaging
        self.identity = {
            'my_id': self.current_id,
            'node_id': self.node_id
        }

    def _init_raft_state(self, node_info: dict):
        """Initialize Raft consensus state"""
        self.state = NodeState.FOLLOWER
        self.cur_num_nodes = len(node_info)
        self.cur_term = 1
        self.voted_for: Optional[int] = None
        self.leader_id: Optional[int] = None
        self.votes_received: Set[int] = set()

    def _init_log_state(self):
        """Initialize log and commit tracking"""
        self.log = [{'term': 1, 'entry': 'INITIAL_ENTRY', 'id': 0}]
        self.commit_index = 0
        self.last_applied_index = 0
        self.last_applied_term = 1
        self.next_entry_id = 1 
        self.log_hash = {}

    
    def _initialize_leader_state(self):
        """Initialize necessary state variables for the leader"""
        self.leader_id = self.current_id
        active_peers = self.check_active_peers()
        self.match_index = {peer_id: 0 for peer_id in active_peers}
        self.next_index = {peer_id: len(self.log) for peer_id in active_peers}
        self.last_heartbeat_times = {peer_id: time.time() for peer_id in active_peers}
        self.logger.info(f"Leader {self.node_id} initialized state for log replication.")

    def _init_timing_config(self):
        """Initialize timing-related configuration"""
        self.heartbeat_interval = 0.05
        self.time_resend = 2.0
        self.last_heartbeat = time.time()

    def _init_network_components(self):
        """Initialize and start network interfaces"""
        self.network_sender = MessageSender(self.identity)
        self.network_receiver = MessageReceiver(self.peer_ids, self.identity)
        
        try:
            self.network_sender.start()
            self.network_receiver.start()
            self.network_sender.check_ready()
        except Exception as e:
            self.logger.error(f"Failed to initialize network components: {e}")
            raise

    def _log_startup_info(self):
        """Log node initialization details"""
        #self.logger.info(f"Starting Node {self.node_id}")
        #self.logger.info(f"My address: {self.current_id}")
        #self.logger.info(f"Node {self.node_id} initialized at {self.current_id}")
        #self.logger.info(f"Initial state: {self.state.value}")

    def _init_logging(self, verbose):
        """Setup logging configuration"""
        self.logger = logging.getLogger(__name__)
        level = logging.DEBUG if verbose else logging.WARNING
        logging.basicConfig(level=level)

    def _init_threading(self):
        """Initialize threading components"""
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._terminate = False
        self.client_queue = Queue()
        self.client_lock = threading.RLock()


    # == CORE OPERARIONS METHODS =====-
    def run(self):
        """Main node operation loop handling state transitions"""
        while not self._terminate:
            try:
                message = self._get_message()
                if message and isinstance(message, StateChangeMsg):
                    self._handle_state_change(message)
                    continue
                    
                self.handle_current_state()
                time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Error in main loop: {e}")
                self.stop()
                break

    def handle_current_state(self):
        """Handle node behavior based on current state"""
        if self.state == NodeState.FOLLOWER:
            self.become_follower()
        elif self.state == NodeState.CANDIDATE:
            self.become_candidate()
        elif self.state == NodeState.LEADER:
            self.become_leader()

    def stop(self):
        """Stop the Raft node gracefully."""
        self._terminate = True
        self.network_sender.terminate()
        self.network_receiver.terminate()


    # == STATE TRANSITION METHODS =====-
    def set_state(self, new_state: NodeState):
        """Update node state with thread safety"""
        with self.client_lock:
            old_state = self.state
            self.state = new_state
            self.logger.info(f"State changed from {old_state} to {new_state}")

    def check_state(self) -> NodeState:
        """Get current node state"""
        with self.client_lock:
            return self.state
    
    def become_candidate(self):
        """
            Responsibilities of a candidate:
            • Start election process
            • Increment term and request votes
            • Check for active peers
            
            State Changes:
            • Sets state to CANDIDATE
            • Increments current term
            • Records self-vote
            • Broadcasts vote requests
            
            Transitions:
            • If no active peers -> Becomes leader
            • If election timeout -> Restarts election
            • If higher term seen -> Becomes follower
        """
        backoff_factor = 1.0 
        while not self._terminate and self.state == NodeState.CANDIDATE:

            # intialize election timeout        
            election_timeout = random.uniform(0.3, 0.6) * backoff_factor
            self.logger.info(f"Starting election with timeout {election_timeout:.3f}s")
            
            # increment term and vote for self
            self.cur_term += 1
            self.voted_for = self.current_id
            self.votes_received = {self.current_id}

            # check for active peers
            active_peers = self.check_active_peers()
            total_active = len(active_peers) + 1
            
            #if no active peers, become leader
            if not active_peers:
                self.logger.info("No active peers - becoming leader")
                self.set_state(NodeState.LEADER)
                return

            # broadcast vote request
            self.request_votes_from_peers()

            election_start = time.time()
            votes_needed = (total_active // 2) + 1
            
            # wait for votes
            while time.time() - election_start < election_timeout:
                message = self._get_message()
                if not message:
                    time.sleep(0.05)
                    continue

                # if we see a leader for our term, step down
                if (message.type in [MsgType.Heartbeat, MsgType.LogAppend] and 
                    message.term >= self.cur_term):
                    self.leader_id = message.sender
                    self.set_state(NodeState.FOLLOWER)
                    return
                    
                #self.logger.debug(f"Received message: {message.type} from {message.sender} (term: {message.term})")
                
                # if we receive a vote request, handle it
                if message.type == MsgType.VoteRequest:
                    self._handle_vote_request_candidate(message)
                    if self.state == NodeState.FOLLOWER:
                        backoff_factor *= 1.5  
                        return

                # if we see a higher term, step down
                if message.term > self.cur_term:
                    self.logger.info(f"Discovered higher term {message.term}. Reverting to follower")
                    self.increment_term(message.term)
                    self.set_state(NodeState.FOLLOWER)
                    return
                    
                # if we receive a vote response, handle it
                if message.type == MsgType.VoteResponse and message.term == self.cur_term:
                    if isinstance(message.results, dict):
                        vote_success = message.results.get('vote_success', False)
                    else:
                        vote_success = getattr(message.results, 'vote_success', False)
                        
                    if vote_success:
                        self.votes_received.add(message.sender)
                        self.logger.info(f"Received vote from {message.sender} ({len(self.votes_received)}/{votes_needed} votes)")
                        
                        if len(self.votes_received) >= votes_needed:
                            self.logger.info(f"Won election with {len(self.votes_received)} votes!")
                            self.set_state(NodeState.LEADER)
                            return
            
            # if we reach the election timeout, increase backoff and restart
            self.logger.debug("Election timeout reached")
            backoff_factor *= 1.5  

    
    def become_leader(self):
        """
        Responsibilities:
        • Manage leader operations and cluster consistency
        • Handle log replication and heartbeats
        • Process messages and client requests
        
        Actions:
        • Initialize leader state and send initial heartbeat
        • Monitor follower health and log consistency
        • Handle incoming messages (votes, acks, requests)
        
        State & Timing:
        • Maintains LEADER state until challenged/terminated
        • Sends periodic heartbeats and checks followers
        """

        self.logger.info(f"Node {self.node_id} became leader for term {self.cur_term}")
        
        # initialize leader state
        self._initialize_leader_state()
        
        # send initial heartbeat and status update
        self.send_heartbeat()
        last_heartbeat = time.time()
        self.replicate_log({'term': self.cur_term, 'entry': 'Leader Status Update', 'id': -1})

        while not self._terminate and self.state == NodeState.LEADER:
            try:
                
                # get last log info for heartbeat
                last_log_idx = self.log_last_idx()
                if isinstance(last_log_idx, str):
                    last_log_idx = int(last_log_idx)
                    print(f"last_log_idx: {last_log_idx}")
                last_log_term = self.log[int(last_log_idx)]['term'] if last_log_idx >= 0 else 0
                
                # send heartbeat if interval passed
                current_time = time.time()
                if (current_time - last_heartbeat) > self.heartbeat_interval:
                    self.send_heartbeat()
                    last_heartbeat = current_time

                # check unresponsive followers
                for peer_id, next_idx in self.next_index.items(): 
                    if next_idx is not None and ((current_time - self.last_heartbeat_times[peer_id]) > self.time_resend):
                        if next_idx < len(self.log):
                            entries = self.log[next_idx:]
                            last_log_idx = max(0, next_idx - 1)
                            last_log_term = self.log[last_log_idx]['term'] if last_log_idx >= 0 else 0  # Fixed: handle edge case
                            self.replicate_to_follower(last_log_idx, last_log_term, entries, peer_id)

                # handle incoming messages
                incoming_message = self._get_message()
                if incoming_message:
                    if incoming_message.direction == MsgDirection.Response:
                        if incoming_message.type == MsgType.Acknowledge:
                            self._handle_acknowledgement(incoming_message)
                        elif incoming_message.type == MsgType.ClientRequest:
                            self._handle_client_request(incoming_message)
                    elif incoming_message.direction == MsgDirection.Request:
                        if incoming_message.type == MsgType.VoteRequest:
                            self._handle_vote_request_leader(incoming_message)
                        if incoming_message.type == MsgType.StateChange:
                            self._handle_state_change(incoming_message)

                #handle client requests
                client_request = self.get_clients_request()
                if client_request is not None:
                    self.replicate_log(client_request)

                time.sleep(0.001)  
            except Exception as e:
                self.logger.error(f"Error in leader loop: {e}", exc_info=True)
                self.logger.error(f"Traceback:\n{traceback.format_exc()}")

    def become_follower(self):
        """
        Responsibilities:
        • Monitor leader heartbeats and timeouts
        • Process incoming messages and requests
        • Handle state transitions
        
        Actions:
        • Set random election timeout
        • Process vote requests and heartbeats
        • Handle log append and commit messages
        
        State & Timing:
        • Maintains FOLLOWER state until timeout/termination
        • Random election timeout (150-300ms)
        • Processes messages with minimal delay
        """
        # set random election timeout
        with self.client_lock:
            election_timeout = random.uniform(0.15, 0.3)
            self.logger.info(f"Follower starting with election timeout {election_timeout:.3f}s")

            self.leader_id = None
            last_heartbeat = time.time()

        # get messages and handle them accordingly
        while not self._terminate and self.state == NodeState.FOLLOWER:
            try:
                incoming_message = self._get_message()
                current_time = time.time()

                # if we get a message, handle it
                if incoming_message:
                    with self.client_lock:
                        if incoming_message.direction == MsgDirection.Request:
                            if incoming_message.type == MsgType.VoteRequest:
                                self._handle_vote_request_follower(incoming_message)
                                last_heartbeat = time.time()
                            elif incoming_message.type == MsgType.Heartbeat:
                                self._handle_heartbeat_follower(incoming_message)
                                last_heartbeat = time.time()
                            elif incoming_message.type == MsgType.LogAppend:
                                self._handle_append_entries_follower(incoming_message)
                            elif incoming_message.type == MsgType.LogCommit:
                                self._handle_committal_follower(incoming_message)

                # check for election timeout
                if (current_time - last_heartbeat) > election_timeout:
                    self.set_state(NodeState.CANDIDATE)
                    return

                time.sleep(0.001)  
            
            except Exception as e:
                self.logger.error(f"Error in follower loop: {e}", exc_info=True)
                self.logger.error(f"Traceback:\n{traceback.format_exc()}")


    # === Message Processing Methods ===
    def _get_message(self):
        """Retrieve and parse next message from the network receiver"""
        raw_message = self.network_receiver.fetch_message()
        if raw_message is not None:
            try:
                return parse_json_message(raw_message)
            except Exception as e:
                self.logger.error(f"Error parsing message: {e}")
                return None
        return None
    
    def _handle_vote_request_follower(self, message):
        """
        Evaluate vote requests and grand votes based on Raft Protocol
        """
        vote_success = False
        
        self.logger.debug(f"Processing vote request from {message.sender} for term {message.term}")
        self.logger.debug(f"My state: term={self.cur_term}, voted_for={self.voted_for}")

        # if message term is higher, update our term and reset vote
        if message.term > self.cur_term:
            self.cur_term = message.term
            self.voted_for = None
            self.logger.debug(f"Updated to higher term {message.term}")

        # grant vote if:
        # message term >= our term
        # we haven't voted this term OR already voted for this candidate
        # candidate's log is at least as up to date
        if (message.term >= self.cur_term and
            (self.voted_for is None or self.voted_for == message.candidate_id) and
            message.last_log_idx >= self.last_applied_index and
            message.last_log_term >= self.last_applied_term):
            
            vote_success = True
            self.voted_for = message.candidate_id
            self.last_heartbeat = time.time()  # reset election timeout
            self.logger.info(f"Granted vote to {message.sender} for term {message.term}")
        else:
            reason = "unknown"
            if message.term < self.cur_term:
                reason = "lower term"
            elif self.voted_for is not None:
                reason = f"already voted for {self.voted_for}"
            elif message.last_log_idx < self.last_applied_index:
                reason = "log not up to date"
            
            self.logger.info(f"Rejected vote request from {message.sender} for term {message.term} ({reason})")

        # send vote response
        vote_results = {
            'term': self.cur_term,
            'vote_success': vote_success
        }
        response = Message(
            type=MsgType.VoteResponse,
            term=self.cur_term,
            direction=MsgDirection.Response,
            sender=self.current_id,
            receiver=message.sender,
            results=vote_results
        )
        
        self.send_message(response)
    
    def _handle_vote_request_leader(self, message):
        "Handles vote request messages that may cause the leader to step down."
        if message.term > self.cur_term:
            self.increment_term(message.term)
            self.send_vote(message.sender)
            self.set_state(NodeState.FOLLOWER)
            if self.verbose:
                print(self.node_id + ': demoting as saw higher term')
            return
        
    def _handle_vote_request_candidate(self, message):
        """Handle vote requests when in candidate state based on Raft Protocol"""
        with self.client_lock:
            #check term, if they have higher than us step down
            if message.term > self.cur_term:
                self.logger.info(f"Higher term {message.term} detected, stepping down")
                self.increment_term(message.term)
                self.set_state(NodeState.FOLLOWER)
                self._handle_vote_request_follower(message)
                return
                
            # check log or index length, if they have higher than us, step down
            if message.term == self.cur_term:
                our_last_term = self.log[self.last_applied_index]['term']
                if message.last_log_term > our_last_term:
                    self.logger.info(f"Same term but higher last log term, stepping down")
                    self.set_state(NodeState.FOLLOWER)
                    self._handle_vote_request_follower(message)
                    return
                    
                # if same log term, check log length
                if message.last_log_term == our_last_term:
                    if message.last_log_idx > self.last_applied_index:
                        self.logger.info(f"Same term and log term but longer log, stepping down")
                        self.set_state(NodeState.FOLLOWER)
                        self._handle_vote_request_follower(message)
                        return
                        
                    # if same log completeness, check timestamp
                    if message.last_log_idx == self.last_applied_index:
                        if message.time_stamp < self.last_heartbeat:  # They started first
                            self.logger.info(f"Equal logs but other candidate started first, stepping down")
                            self.set_state(NodeState.FOLLOWER)
                            self._handle_vote_request_follower(message)
                            return
            
            # we have higher priority term/index/time, reject their vote
            self.send_vote(message.sender, False)

        
    def _handle_heartbeat_follower(self, message):
        """Handles heartbeat messages from the leader with proper state updates."""
        with self.client_lock:
            if message.term > self.cur_term:
                self.increment_term(message.term)
                self.voted_for = None
                
            # add state change handling
            if message.type == MsgType.StateChange:
                self._handle_state_change(message)
                return
            
            # only update leader_id if the message has it
            if hasattr(message, 'leader_id') and message.leader_id is not None:
                self.leader_id = message.leader_id

            # send heartbeat response back to leader
            response = {
                'type': MsgType.Heartbeat.value,
                'term': self.cur_term,
                'time_stamp': int(time.time()),
                'sender': self.current_id,
                'receiver': message.sender,
                'direction': MsgDirection.Response.value,
                'results': {'success': True},
                'leader_id': self.leader_id,
                'last_log_idx': self.last_applied_index,
                'last_log_term': self.last_applied_term,
                'entries': [],
                'leader_commit': self.commit_index
            }
            self.network_sender.queue_message(response)

            # send any pending client requests to leader
            client_request = self.get_clients_request()
            if client_request is not None and self.leader_id:
                self.send_clients_request(self.leader_id, client_request)


    def _handle_append_entries_follower(self, message):

        """Handles append entries requests from the leader."""
        # reply false if message term is less than cur_term
        with self.client_lock:
            if message.term < self.cur_term:
                self.send_acknowledge(message.leader_id, False)
                return

            # reply false if log doesn't contain entry at last_log_idx with last_log_term
            if not self.validate_log_entry(message.last_log_idx, message.last_log_term):
                self.send_acknowledge(message.leader_id, False)
                return
        
            # append the entry based on leader's commit status
            if message.leader_commit > self.commit_index:
                self.log_entry(message.entries, commit=True, 
                                last_log_idx=message.last_log_idx)
            else:
                self.log_entry(message.entries, commit=False, 
                                last_log_idx=message.last_log_idx)
            
            self.send_acknowledge(message.leader_id, True)


    def _handle_committal_follower(self, message):
        """Handles commit messages from the leader."""
        self.commit_entry(message.last_log_idx, message.last_log_term)


    def _handle_acknowledgement(self, message):
        """Handle acknowledgement messages from followers"""
        try:
            sender = message.sender
            
            if sender not in self.next_index:
                self.logger.warning(f"Received ack from unknown sender: {sender}")
                return
                
            if self.next_index[sender] is not None:
                # access the nested dictionary correctly
                if isinstance(message.results, dict) and message.results.get('success', False):
                    # Success case
                    self.match_index[sender] = self.next_index[sender]
                    self.next_index[sender] = self.log_last_idx() + 1
                    #self.logger.debug(f"Successfully updated indices for {sender}")
                else:
                    # failure case
                    self.next_index[sender] = max(0, self.next_index[sender] - 1)
                    #self.logger.debug(f"Decremented next_index for {sender}")
                    
            self.find_commit_point()
            
        except Exception as e:
            self.logger.error(f"Error in _handle_acknowledgement: {e}")
            self.logger.error(f"Message content: {vars(message)}")
            self.logger.error(f"Results type: {type(message.results)}")
            self.logger.error(f"Results content: {message.results}")
            raise

    
    def _handle_client_request(self):
        "Processes a client request by broadcasting append entries to followers."
        try:
            client_request = self.client_queue.get(block=False)
        except Empty:
            client_request =  None
            
        if client_request:
            self.replicate_log(client_request)
 
    def _handle_state_change(self, message):
        """Handle state change request"""
        try:
            self.logger.info(f"Processing state change request from {message.sender}")
            self.logger.info(f"Current state: {self.state.value}, Requested state: {message.new_state}")
            
            # convert string state to enum
            try:
                new_state = NodeState(message.new_state)
            except ValueError:
                self.logger.error(f"Invalid state requested: {message.new_state}")
                return

            # change state
            self.set_state(new_state)
            
            # send acknowledgment
            response = {
                'type': MsgType.Acknowledge.value,
                'term': self.cur_term,
                'sender': self.current_id,
                'receiver': message.sender,
                'direction': MsgDirection.Response.value,
                'results': {
                    'success': True,
                    'new_state': self.state.value
                }
            }
            self.network_sender.queue_message(response)
            self.logger.info(f"State change complete. New state: {self.state.value}")
            
        except Exception as e:
            self.logger.error(f"Error handling state change: {e}")
            self.logger.exception("Traceback:")
    
    def get_clients_request(self):
        """get the most recent client request"""
        try:
            return self.client_queue.get(block=False)
        except Empty:
            return None

    # === Message Sending Methods ===
    def send_vote_request(self, receiver: Optional[str] = None):
        "Send vote request to other nodess"
        with self.client_lock:
            message = VoteRequestMsg(
                type=MsgType.VoteRequest,
                term=self.cur_term,
                sender=self.current_id,
                receiver=receiver,
                direction=MsgDirection.Request,
                candidate_id=self.current_id,  
                last_log_idx=self.last_applied_index,
                last_log_term=self.last_applied_term,
            )
            #self.logger.debug(f"Sending vote request to {receiver} for term {self.cur_term}")
            self.send_message(message)

    
    def send_vote(self, candidate: int, vote_success: bool = True):
        """
        Responds to a vote request, granting or denying the vote.
        """
        with self.client_lock:
            message = VoteRequestMsg(
                type=MsgType.VoteResponse,
            term=self.cur_term,
            sender=self.current_id,
            receiver=candidate,
            direction=MsgDirection.Response,
            candidate_id=candidate,
            last_log_idx=self.last_applied_index,
            last_log_term=self.last_applied_term,
            results= VoteResults(
                term=self.cur_term,
                vote_success=vote_success
            )
        )
        self.send_message(message)
        self.voted_for = candidate if vote_success else None
    
    def send_heartbeat(self):
        """Send heartbeat to all peers"""

        try:
            # create base heartbeat message
            heartbeat = {
                'type': MsgType.Heartbeat.value,
                'term': self.cur_term,
                'time_stamp': int(time.time()),
                'sender': self.current_id,
                'direction': MsgDirection.Request.value,
                'leader_id': self.leader_id,
                'last_log_idx': self.last_applied_index,
                'last_log_term': self.last_applied_term,
                'entries': [],
                'leader_commit': self.commit_index
            }

            # send to each peer
            for peer_id in self.peer_ids:
                try:
                    # customize message for each peer
                    heartbeat['receiver'] = peer_id
                    self.network_sender.queue_message(heartbeat.copy())  
                except Exception as e:
                    self.logger.error(f"Error sending heartbeat to {peer_id}: {e}")

        except Exception as e:
            self.logger.error(f"Error in send_heartbeat: {e}")
    
    def send_clients_request(self, receiver, entry):
        self.last_applied_index = len(self.log) - 1
        self.last_applied_term = self.log[self.last_applied_index]['term']

        message = LogAppendMsg(
            type = MsgType.ClientRequest,
            term = self.cur_term,
            sender = self.current_id,
            receiver = receiver,
            direction = MsgDirection.Response,
            leader_id = self.leader_id,
            last_log_idx = self.last_applied_index,
            last_log_term = self.last_applied_term,
            entries = entry,
            leader_commit = self.commit_index
        )
        self.send_message(message)
    
    def send_commits(self, index, receiver=None):
        """ Notify followers of commit updates and maintain commit index consistency """
        try:
            message = LogAppendMsg(
                type=MsgType.LogCommit,
                term=self.cur_term,
                sender=self.current_id,
                receiver=receiver,  # none = broadcast
                direction=MsgDirection.Request,
                leader_id=self.current_id,
                last_log_idx=index, 
                last_log_term=self.log[index]['term'],  
                entries=None,
                leader_commit=index  
            )
            self.send_message(message)

        except Exception as e:
            self.logger.error(f"Error sending commit: {e}")
        
    def send_acknowledge(self, receiver, success, entry=None):
        message = LogAppendMsg(
            type = MsgType.Acknowledge,
            term = self.cur_term,
            sender = self.current_id,
            receiver = receiver,
            direction = MsgDirection.Response,
            leader_id =self.leader_id ,
            last_log_idx = self.last_applied_index,
            last_log_term = self.last_applied_term,
            entries = entry,
            leader_commit = self.commit_index, 
            results = LogAppendResults(
                term = self.cur_term,
                success = success
            ) 
        )
        self.send_message(message)
    
    def replicate_to_follower(self, index, term, entries, receiver=None):
        """Replicate log entries to s followers"""
        message = LogAppendMsg(
            type=MsgType.LogAppend,
            leader_id=self.current_id,
            sender=self.current_id,
            receiver=receiver,  
            term=self.cur_term,
            direction=MsgDirection.Request,
            last_log_idx=index,      
            last_log_term=term,
            entries=entries,
            leader_commit=self.commit_index
        )
        self.send_message(message)
    
    def send_message(self, message):
        "Send a message using the network sender"
        self.network_sender.queue_message(message.encode_json())
    

    # === Log Management Methods ===
    def replicate_log(self, entry):
        """Replicate log entries to all nodes as leader"""
        try:
            # get current index BEFORE appending
            current_idx = len(self.log) - 1
            current_term = self.log[current_idx]['term']
            
            # assign new ID to entry if it doesn't have one
            if isinstance(entry, dict):
                if 'id' not in entry or entry['id'] == -1:
                    entry['id'] = self.next_entry_id
                    self.next_entry_id += 1
            
            #append the new entry
            self.log.append(entry)
            new_idx = len(self.log) - 1
            
            self.logger.debug(f"Replicating entry at index {new_idx}: {entry}")

            #replicate to peers
            for peer_id, next_idx in self.next_index.items():
                if next_idx is not None:
                    entries = [entry]
                    self.replicate_to_follower(current_idx, current_term, entries, peer_id)
                    self.next_index[peer_id] = new_idx + 1

            # Update leader's own tracking
            self.next_index[self.current_id] = None
            self.match_index[self.current_id] = new_idx
            
        except Exception as e:
            self.logger.error(f"Error in replicate_log: {e}", exc_info=True)


    def log_entry(self, entry, commit=False, last_log_idx=None):
        """Log entry management"""
        try:
            with self.client_lock:
                last_log_idx = max(0, int(last_log_idx)) if last_log_idx is not None else len(self.log) - 1
                last_log_term = self.log[last_log_idx]['term']
                
                #handle entry being list or single entry
                if isinstance(entry, list):
                    for e in entry:
                        e['id'] = self.next_entry_id
                        self.next_entry_id += 1
                        self.log.append(e)
                        self.log_hash[e['id']] = e
                else:
                    entry['id'] = self.next_entry_id
                    self.next_entry_id += 1
                    self.log.append(entry)
                    self.log_hash[entry['id']] = entry
                
                if commit:
                    self.commit_entry(len(self.log) - 1, entry['term'])

                return last_log_idx, last_log_term

        except Exception as e:
            self.logger.error(f"Error in log_entry: {e}")
            return None, None
        
    
    def commit_entry(self, entry_index, entry_term=None):
        """Commit entry to state machine"""
        try:
            # debug log to see 
            #self.logger.debug(f"commit_entry called with index: {entry_index} (type: {type(entry_index)})")
            
            if isinstance(entry_index, str):
                entry_index = int(entry_index)
                
            self.last_applied_index = entry_index
            self.last_applied_term = entry_term
            
            with self.client_lock:
                self.commit_index = entry_index
                
        except Exception as e:
            self.logger.error(f"Error in commit_entry - index: {entry_index}, type: {type(entry_index)}")
            self.logger.error("Traceback:", exc_info=True)
            raise
    
    def replicate_commmit_entries(self, idx):
        """replicate commit entries to all nodes"""

        # commit yourself
        self.commit_entry(idx, self.log[idx]['term'])

        # commit everybody else
        for follower_id, match_idx in self.match_index.items():
            if (match_idx >= idx):
                self.send_commits(idx, follower_id)

    

    # === Utility Methods ===
    def log_last_idx(self):
        """return last index of log """
        return len(self.log) - 1
    
    def get_node_idx(self,node_addrs):
        return self.peer_ids.index(node_addrs)
    

    def check_active_peers(self):
        """Check which peers are currently active"""

        active_peers = []
        total_peers = len(self.peer_ids)
        
        # send heartbeat to all peers
        for peer_id in self.peer_ids:
            heartbeat = {
                'type': MsgType.Heartbeat.value,
                'term': self.cur_term,
                'direction': MsgDirection.Request.value,
                'sender': self.current_id,
                'receiver': peer_id
            }
            self.network_sender.queue_message(heartbeat)
        
        # W\wait for responses with proper timeout
        start_time = time.time()
        timeout = 0.2  #
        
        while time.time() - start_time < timeout:
            msg = self.network_receiver.fetch_message()
            if msg and msg.get('sender') in self.peer_ids:
                if msg.get('sender') not in active_peers:
                    active_peers.append(msg.get('sender'))
                    
            # if we found majority of peers, we can stop waiting
            if len(active_peers) >= total_peers // 2:
                break
                
            time.sleep(0.05)  # small sleep to prevent CPU spinning
        
        if not active_peers:
            self.logger.warning("No peers responded - waiting longer before leadership")
            time.sleep(random.uniform(0.5, 1.0))  # Additional random delay
            
            # double-check for peer responses
            msg = self.network_receiver.fetch_message()
            if msg and msg.get('sender') in self.peer_ids:
                active_peers.append(msg.get('sender'))
        
        self.logger.info(f"Found {len(active_peers)} active peers out of {total_peers}")
        return active_peers


    def validate_log_entry(self, last_log_idx, last_log_term):
        """Verify if log contains entry at last_log_idx with matching term."""
        with self.client_lock:
            # handle None case
            if last_log_idx is None:
                self.logger.warning("Received None for last_log_idx in validate_log_entry")
                return False
                
            # convert to int if string
            if isinstance(last_log_idx, str):
                last_log_idx = int(last_log_idx)
                
            # check if index exists
            if last_log_idx >= len(self.log):
                return False
                
            # check term match
            return self.log[last_log_idx]['term'] == last_log_term
        
    def request_votes_from_peers(self):
        """Send vote requests to all peers"""
        for peer_id in self.peer_ids:
            self.send_vote_request(peer_id)


    def _load_config(self, config_path: str, node_id: int) -> dict:
        """Load node configuration from a file """
        try:
            import json
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config file: {e}")
            raise
    
    def find_commit_point(self):
        """Find the max committable point (index) based on majority replication and send commit entries"""
        log_count = sorted([int(i) for i in self.match_index.values() if i is not None], reverse=True)
        max_commit_idx = 0

        for idx in log_count:
            replicated_on = sum(1 if idx <= i else 0 for i in log_count)
            if replicated_on >= (self.cur_num_nodes// 2 + 1):
                max_commit_idx = idx
                break

        if max_commit_idx > self.commit_index:
            self.replicate_commmit_entries(max_commit_idx)

    def increment_term(self, new_term=None):
        """Update the current term. if input is none, auto add 1, else will change to whatever is input"""
        with self.client_lock:
            if new_term is not None:
                self.cur_term = new_term
            else:
                self.cur_term += 1
            self.voted_for = None
            self.votes_received.clear()
            self.logger.info(f"Term updated to {self.cur_term}")