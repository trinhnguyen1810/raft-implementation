from enum import Enum
import time

class MsgType(Enum):
    VoteRequest = "VOTE_REQUEST"
    VoteResponse = "VOTE_RESPONSE"
    Acknowledge = "ACKNOWLEDGE"
    Heartbeat = "HEARTBEAT"
    LogAppend =  "LOG_APPEND"
    LogCommit = "LOG_COMMIT"
    ClientRequest = "CLIENT_REQUEST"
    StateChange = "STATE_CHANGE"

class MsgDirection(Enum):
    
    Request = "REQUEST"   
    Response = "RESPONSE"  

class Message:
    def __init__(self, type, term, direction, sender, receiver, results=None, message=None):
        self.time_stamp = int(time.time())
        self.type = type
        self.direction = direction
        self.term = term
        self.sender = sender
        self.receiver = receiver
        self.results = results
        
        if message is not None:
            if not isinstance(message, dict):
                raise ValueError("Message must be a dictionary")
            self.decode_json(message)


    def decode_json(self, message):
        required_fields = ['type', 'term', 'sender', 'receiver', 'direction']
        for field in required_fields:
            if field not in message:
                raise ValueError(f"Missing required field: {field}")
                
        self.type = MsgType(message['type'])
        self.term = int(message['term'])
        self.time_stamp = int(message.get('time_stamp', time.time()))
        self.sender = message['sender']
        self.receiver = message['receiver']
        self.direction = MsgDirection(message['direction'])

        if 'results' in message and message['results']:
            if not isinstance(message['results'], dict):
                raise ValueError("Results must be a dictionary")
            if self.type == MsgType.VoteRequest:
                self.results = VoteResults(message=message['results'])
            elif self.type == MsgType.LogAppend:
                self.results = LogAppendResults(message=message['results'])

    def encode_json(self):
        """Encode message to JSON format"""
        return {
            'type': self.type.value,
            'term': self.term,
            'time_stamp': self.time_stamp,
            'sender': self.sender,
            'receiver': self.receiver,
            'direction': self.direction.value,
            'results': self.results.encode_json() if hasattr(self.results, 'encode_json') else self.results
        }

class VoteResults:
    def __init__(self, term=None, vote_success=None, message=None):
        if message is not None:
            if not isinstance(message, dict):
                raise ValueError("Results message must be a dictionary")
            self.decode_json(message)
        else:
            self.term = term
            self.vote_success = bool(vote_success)

    def decode_json(self, message):
        self.term = message['term']
        self.vote_success = bool(message['vote_success'])

    def encode_json(self):
        return {
            'term': self.term,
            'vote_success': self.vote_success
        }
    
class LogAppendResults:
    def __init__(self, term=None, success=None, message=None):
        if message is not None:
            if not isinstance(message, dict):
                raise ValueError("Results message must be a dictionary")
            self.decode_json(message)
        else:
            self.term = term
            self.success = bool(success)

    def decode_json(self, message):
        self.term = message['term']
        self.success = bool(message['success'])

    def encode_json(self):
        return {
            'term': self.term,
            'success': self.success
        }

class VoteRequestMsg(Message):
    def __init__(self, type=None, term=None, sender=None, receiver=None, direction=None, results=None, candidate_id=None, 
                last_log_idx=None, last_log_term=None, message=None):
        super().__init__(
            type=type, 
            term=term,
            direction=direction,
            sender=sender,
            receiver=receiver,
            results=results,
            message=message
        )
        if message is not None:
            self.decode_json(message)
        else:
            self.candidate_id = candidate_id
            self.last_log_idx = last_log_idx
            self.last_log_term = last_log_term
            self.results = None

    def decode_json(self, message):
        super().decode_json(message)
        self.candidate_id = message['candidate_id']
        self.last_log_idx = message['last_log_idx']
        self.last_log_term = message['last_log_term']

    def encode_json(self):
        message = super().encode_json()
        message.update({
            'candidate_id': self.candidate_id,
            'last_log_idx': self.last_log_idx,
            'last_log_term': self.last_log_term
        })
        return message

class LogAppendMsg(Message):
    def __init__(self, type=None, term=None, sender=None, receiver=None, direction=None, results=None, leader_id=None,
                last_log_idx=None, last_log_term=None, entries=None, leader_commit=None, message=None):
        # First, declare all the attributes that this class will use
        self.leader_id = None
        self.last_log_idx = None
        self.last_log_term = None
        self.entries = []
        self.leader_commit = None
        self.results = None

        # Initialize base Message class
        super().__init__(
            type=type,
            term=term,
            direction=direction,
            sender=sender,
            receiver=receiver,
            results=results,
            message=message
        )

        # If a message dict is provided, decode it
        if message is not None:
            self.decode_json(message)
        else:
            # Otherwise use the provided values
            self.leader_id = leader_id
            self.last_log_idx = last_log_idx
            self.last_log_term = last_log_term
            self.entries = entries or []
            self.leader_commit = leader_commit
            self.results = results

    def decode_json(self, message):
        """Decode JSON message into object attributes"""
        super().decode_json(message)
        # Use get() with defaults
        self.leader_id = message.get('leader_id')
        self.last_log_idx = message.get('last_log_idx', 0)
        self.last_log_term = message.get('last_log_term', 0)
        self.entries = message.get('entries', [])
        self.leader_commit = message.get('leader_commit', 0)

    def encode_json(self):
        """Encode object attributes into JSON message"""
        message = super().encode_json()
        message.update({
            'leader_id': self.leader_id,
            'last_log_idx': self.last_log_idx,
            'last_log_term': self.last_log_term,
            'entries': self.entries,
            'leader_commit': self.leader_commit
        })
        return message

class StateChangeMsg(Message):
    def __init__(self, type=None, term=None, sender=None, receiver=None, direction=None, 
                 new_state=None, message=None):
        super().__init__(
            type=type,
            term=term,
            direction=direction,
            sender=sender,
            receiver=receiver,
            message=message
        )
        
        if message is not None:
            self.decode_json(message)
        else:
            self.new_state = new_state

    def decode_json(self, message):
        super().decode_json(message)
        self.new_state = message.get('new_state')

    def encode_json(self):
        message = super().encode_json()
        message.update({
            'new_state': self.new_state
        })
        return message

class LogCommitMsg(Message):
    def __init__(self, type=None, term=None, sender=None, receiver=None, direction=None, 
                 last_log_idx=None, last_log_term=None, message=None):
        self.last_log_idx = None
        self.last_log_term = None
        
        super().__init__(
            type=type,
            term=term,
            direction=direction,
            sender=sender,
            receiver=receiver,
            message=message
        )
        
        if message is not None:
            self.decode_json(message)
        else:
            self.last_log_idx = last_log_idx
            self.last_log_term = last_log_term
            
    def decode_json(self, message):
        """Decode JSON message into object attributes"""
        super().decode_json(message)
        self.last_log_idx = message.get('last_log_idx')
        self.last_log_term = message.get('last_log_term')
        
    def encode_json(self):
        """Encode object attributes into JSON message"""
        message = super().encode_json()
        message.update({
            'last_log_idx': self.last_log_idx,
            'last_log_term': self.last_log_term
        })
        return message

def parse_json_message(json_message):
    """Convert a JSON dict into the appropriate Message object"""
    if json_message is None:
        return None
        
    try:
        type = json_message.get('type')
        if type == MsgType.VoteRequest.value:
            return VoteRequestMsg(message=json_message)
        elif type in [MsgType.LogAppend.value, MsgType.Heartbeat.value]:
            return LogAppendMsg(message=json_message)
        elif type == MsgType.LogCommit.value:  
            return LogCommitMsg(message=json_message)
        elif type == MsgType.StateChange.value:
            return StateChangeMsg(message=json_message)
        else:
            return Message(
                type=MsgType(type),
                term=json_message['term'],
                direction=MsgDirection(json_message['direction']),
                sender=json_message['sender'],
                receiver=json_message['receiver'],
                results=json_message.get('results'),
                message=None
            )
    except Exception as e:
        print(f"Error parsing message: {e}, Message: {json_message}")
        raise