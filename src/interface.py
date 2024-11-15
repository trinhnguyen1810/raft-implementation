import zmq
import time
import multiprocessing
from queue import Empty

from message import MsgType

class MessageSender(multiprocessing.Process):
    def __init__(self, config):
        super(MessageSender, self).__init__()
        
        # Network configuration
        self.current_id = config['my_id']
        
        # Timing settings
        self.startup_delay = 1.0
        self.process_delay = 0.0001
        
        # Message queue
        self.outgoing_queue = multiprocessing.Queue()
        
        # Control flags
        self.is_ready = multiprocessing.Event()
        self.should_stop = multiprocessing.Event()

    def terminate(self):
        self.should_stop.set()

    def run(self):
        zmq_context = zmq.Context()
        publisher = zmq_context.socket(zmq.PUB)
        
        # Attempt connection
        while True:
            try:
                publisher.bind(f"tcp://{self.current_id}")
                break
            except zmq.ZMQError:
                time.sleep(0.1)

        # Initialize connection
        time.sleep(self.startup_delay)
        self.is_ready.set()

        # Main sending loop
        while not self.should_stop.is_set():
            try:
                publisher.send_json(self.outgoing_queue.get_nowait())
            except Empty:
                try:
                    time.sleep(self.process_delay)
                except KeyboardInterrupt:
                    break
            except KeyboardInterrupt:
                break

        # Cleanup
        publisher.unbind(f"tcp://{self.current_id}")
        publisher.close()

    def queue_message(self, data):
        self.outgoing_queue.put(data)
    
    def check_ready(self):
        while not self.is_ready.is_set():
            time.sleep(0.1)
        return True

class MessageReceiver(multiprocessing.Process):
    def __init__(self, peer_ids, config):
        super(MessageReceiver, self).__init__()
        self.peer_ids = peer_ids
        self.config = config
        self.startup_delay = 1.0
        self.incoming_queue = multiprocessing.Queue()
        self.should_stop = multiprocessing.Event()
        
        # Add manager address
        self.manager_addr = "tcp://127.0.0.1:5555"  # Match the manager's bind port

    def terminate(self):
        self.should_stop.set()

    def run(self):
        zmq_context = zmq.Context()
        subscriber = zmq_context.socket(zmq.SUB)
        subscriber.setsockopt(zmq.SUBSCRIBE, b'')
        
        # Connect to peers
        for network_addr in self.peer_ids:
            subscriber.connect(f"tcp://{network_addr}")
            print(f"Connected to peer: {network_addr}")
            
        # Connect to manager
        subscriber.connect(self.manager_addr)
        print(f"Connected to manager at: {self.manager_addr}")

        poll_handler = zmq.Poller()
        poll_handler.register(subscriber, zmq.POLLIN)

        time.sleep(self.startup_delay)

        while not self.should_stop.is_set():
            try:
                events = dict(poll_handler.poll(100))
                if subscriber in events and events[subscriber] == zmq.POLLIN:
                    try:
                        data = subscriber.recv_json()
                        #print(f"Received raw message: {data}")  # Debug line
                        
                        if data.get('type') == MsgType.StateChange.value:
                            print(f"MessageReceiver: Received StateChange message: {data}")
                            # Ensure we process state change messages
                            if data['receiver'] == self.config['my_id'] or data['receiver'] is None:
                                print(f"Queuing state change message")
                                self.incoming_queue.put(data)
                            else:
                                print(f"State change not for us. Target: {data['receiver']}, Our ID: {self.config['my_id']}")
                        
                        # Process other messages as before
                        required_fields = ['receiver', 'sender', 'term', 'type', 'direction']
                        if all(field in data for field in required_fields):
                            if data['receiver'] == self.config['my_id'] or data['receiver'] is None:
                                self.incoming_queue.put(data)
                        else:
                            print(f"Malformed message received: Missing required fields - {data}")
                    except ValueError as e:
                        print(f"Invalid JSON message received: {e}")
                    except Exception as e:
                        print(f"Error processing message: {e}")
            except KeyboardInterrupt:
                break

        subscriber.close()

    
    def fetch_message(self):
        try:
            return self.incoming_queue.get_nowait()
        except Empty:
            return None