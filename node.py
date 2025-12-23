import hashlib
from datetime import datetime, timezone
import xmlrpc.client
import sys
from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import threading
import time
from socketserver import ThreadingMixIn
import math
import random
import json
import socket
import os
import signal

"""
A honeycomb node. Created by Shibali Mishra, Ulemj Munkthur, and Caroline Berney.
"""

REGION = {
    "54.205.35.150": "Virginia",
    "54.226.158.73": "Virginia",
    "107.23.190.249": "Virginia",
    "44.220.135.165": "Virginia",

    "3.144.239.184": "Ohio",
    "18.222.158.47": "Ohio",

    "54.151.0.141": "California",
    "54.67.64.203": "California",

    "35.94.113.38": "Oregon",
    "54.185.198.251": "Oregon",

    "16.28.28.111": "Cape Town",
    "13.246.226.139": "Cape Town",

    "18.167.36.13": "Hong Kong",
    "18.163.186.77": "Hong Kong",

    "18.60.45.253": "Hyderabad",
    "40.192.121.231": "Hyderabad",

    "108.137.97.235": "Jakarta",
    "43.218.113.71": "Jakarta",

    "43.216.125.191": "Malaysia",
    "43.217.242.231": "Malaysia",

    "16.50.236.94": "Melbourne",
    "16.26.255.39": "Melbourne",

    "3.110.176.219": "Mumbai",
    "3.110.167.225": "Mumbai",

    "3.102.229.218": "New Zealand",
    "3.103.17.124": "New Zealand",

    "13.208.248.99": "Osaka",
    "56.155.140.41": "Osaka",

    "3.38.201.47": "Seoul",
    "3.34.41.31": "Seoul",

    "3.1.24.159": "Singapore",
    "13.229.151.194": "Singapore",

    "3.25.230.49": "Sydney",
    "52.63.227.53": "Sydney",

    "43.212.249.10": "Taipei",
    "43.212.6.235": "Taipei",

    "43.209.178.31": "Thailand",
    "43.208.245.169": "Thailand",

    "35.72.34.224": "Tokyo",
    "43.206.93.48": "Tokyo",

    "15.223.217.3": "Canada (Central)",
    "3.96.155.48": "Canada (Central)",

    "56.112.60.241": "Canada (Calgary)",
    "40.177.64.108": "Canada (Calgary)",

    "35.159.30.5": "Frankfurt",
    "3.71.90.78": "Frankfurt",

    "18.202.35.151": "Ireland",
    "54.216.140.13": "Ireland",

    "3.8.200.149": "London",
    "13.40.143.174": "London",

    "15.160.118.198": "Milan",
    "15.161.241.238": "Milan",

    "51.45.6.210": "Paris",
    "15.188.53.94": "Paris",

    "51.92.61.215": "Spain",
    "51.92.88.18": "Spain",

    "13.53.200.129": "Stockholm",
    "13.60.227.179": "Stockholm",

    "51.96.139.83": "Zurich",
    "16.62.233.163": "Zurich",

    "78.12.246.123": "Mexico",
    "78.13.53.95": "Mexico",

    "157.175.47.64": "Bahrain",
    "157.175.160.27": "Bahrain",

    "51.112.145.242": "UAE",
    "40.172.112.126": "UAE",

    "51.16.152.59": "Tel Aviv",
    "51.17.154.141": "Tel Aviv",

    "18.228.199.61": "Sao Paulo",
    "56.124.108.117": "Sao Paulo"
}

"""
Wrapper class to allow threading for our XML-RPC node servers. We need this
because, by default, python simple XML-RPC does not allow multithreads.
"""
class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    
    daemon_threads = True

    def handle_error(self, request, client_address):
        exc_type, exc, tb = sys.exc_info()
        if exc_type in (BrokenPipeError, ConnectionResetError):
            return
        super().handle_error(request, client_address)

RPC_TIMEOUT = 5.0
"""
Helper class allowing us to use timeouts on XML-RPC calls.
"""
class TimeoutTransport(xmlrpc.client.Transport):
    
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    # overrides internal Transport method make_connection to configure timeout
    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn

"""
Wrapper function for xmlrpc.client.ServerProxy allowing us to use 
timeouts to detect node failures. XML-RPC requests trigger a timeout if 
they are not successfully answered within 2 seconds.
"""
def rpc_proxy(url):
    return xmlrpc.client.ServerProxy(
        "http://" + url,
        transport=TimeoutTransport(RPC_TIMEOUT),
        allow_none=True
    )

"""
Contains functionality for an individual honeycomb node.
"""
class Node:
    
    # class constants
    M_BIT = 32 # chose M for negligible chance of node ID collision
    NUM_SUCC = 5 # number of successors in node's successor list
    FLUSH_THRESHOLD = 200 # number of messages a node can receive before flush to disk triggered
    TO_FLUSH = 150 # number of messages to write to disk when flushing
    ATTEMPTS = 2 # number of RPC attempts before timeout 
    RETRY_SLEEP = 0.2 # time to sleep before retrying again

    # initializes node object
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.url = f"{ip}:{port}"
        self.id = self.id_sha_hash(self.url) # hashed identifier for this node's location on the Chord ring
        self.successors = [(self.id, self.url)] # this node's successors on the ring
        self.predecessor = None # this node's predecessor on the ring
        self.finger = None # this node's finger, located approximately halfway across the ring
        self.messages = [] # list of message tuples with format (time, message_id, content)
        self.messages_set = set() # set of message_id strings, used to track which messages we have
        self.lock = threading.Lock() # class-level lock
        self.flush_in_progress = False # flag for flushing status
        self.join_finished = False # flag for join status

    """
    Runs stabilize on node approximately every second.
    """
    def run_stabilizer(self):
        while True:
            try:
                self.stabilize()
            except Exception as e:
                print("Stabilize exception:", e)
            time.sleep(1.0 + random.uniform(-0.4, 0.4)) # randomized for better network performance

    """
    Runs check_predecessor on node approximately every second.
    """
    def run_check_predecessor(self):
        while True:
            try:
                self.check_predecessor()
            except Exception as e:
                print("Check predecessor exception:", e)
            time.sleep(1.0 + random.uniform(-0.4, 0.4)) # randomized for better network performance
    
    """
    Runs fix_finger on node approximately every second.
    """
    def run_fix_fingers(self):
        while True:
            try:
                self.fix_fingers()
            except Exception as e:
                print("Fix fingers exception:", e)
            time.sleep(1.0 + random.uniform(-0.4, 0.4)) # randomized for better network performance

    """
    Starts threaded XML_RPC node server, starts listening thread, starts 
    ongoing threads for each periodic call, creates new thread for each 
    incoming XML-RPC request.
    """
    def start_node(self):
        try:
            server = ThreadedXMLRPCServer(
                ("0.0.0.0", self.port),
                allow_none=True,
                logRequests=False
            )
            server.register_instance(self)
            self.server = server
            print(f"[Node {self.url}] XML-RPC server started")

            # puts server on listening mode forever for requests
            t = threading.Thread(target=server.serve_forever, daemon=True)

            # each RPC call to a node makes a new thread
            t.start()
            self.server_thread = t

            # each periodic call runs on its own thread for performance
            stabilizer = threading.Thread(target=self.run_stabilizer, daemon=True)
            stabilizer.start()

            predecessor_check = threading.Thread(target=self.run_check_predecessor, daemon=True)
            predecessor_check.start()

            finger = threading.Thread(target=self.run_fix_fingers, daemon=True)
            finger.start()

            # do nothing, but keep the main process alive as long as the node is running
            while True:
                time.sleep(1) # wake often enough to detect signal handlers
        
        except Exception as e:
            print("NodeServer exception:", e)

    """
    Returns the string result of SHA-1 hash of given URL "ip:port" modded by 
    2^M_BIT as described in Chord. Our choice of M_BIT = 32 ensures 
    negligible  (less than one in a million) chance of node ID collisions.
    """
    def id_sha_hash(self, url):
        h = int(hashlib.sha1(url.encode("utf-8")).hexdigest(), 16)
        return str(h % (2 ** Node.M_BIT))

    """
    Returns message_ID, which is the int SHA-1 hash of message, which is in
    turn a string of the format "message_content,message_time,self.id". 
    """
    def message_sha_hash(self, message):
        return int(hashlib.sha1(message.encode('utf-8')).hexdigest(), 16)

    """
    Safely returns a copy of node's successor list with the format
    [(successor_id, successor_url)].
    """
    def get_successors(self):
        with self.lock:
            return list(self.successors)

    """
    Safely returns a copy of the tuple representing a node's predecessor 
    with the format (predecessor_id, predecessor_url).
    """
    def get_predecessor(self):
        with self.lock:
            return None if self.predecessor is None else tuple(self.predecessor)

    """
    Safely returns a copy of the tuple representing a node's finger with 
    the format (finger_id, finger_url).
    """
    def get_finger(self):
        with self.lock:
            return None if self.finger is None else tuple(self.finger)

    """
    Safely returns a copy of self.messages with the format 
    [(message_time, message_id, message_content)]
    """
    def get_messages(self):
        with self.lock:
            return list(self.messages)

    """
    Safely returns a tuple containing copies of self.messages and 
    self.messages_set.
    """
    def get_message_info(self):
        # sets are not serializable so we convert copy of messages_set into list
        with self.lock:
            return (list(self.messages), list(self.messages_set))    

    """
    Safely sets self.successors to provided successor_list, which is assumed 
    to be of the format [(successor_id, successor_url)] with a length less 
    than NUM_SUCC
    """
    def set_successors(self, successor_list):
        with self.lock:
            self.successors = successor_list

    """
    Safely sets self.predecessor to provided predecessor, which is assumed 
    to be of the format (predecessor_id, predecessor_url).
    """
    def set_predecessor(self, predecessor):
        with self.lock:
            self.predecessor = predecessor
    
    """
    Safely sets self.finger to provided finger, which is assumed to be of 
    the format (finger_id, finger_url).
    """
    def set_finger(self, finger):
        with self.lock:
            self.finger = finger

    """
    Safely sets self.messages to provided message_list, which is assumed 
    to be of the format [(message_time, message_id, message_content)]
    """
    def set_messages_list(self, message_list):
        with self.lock:
            self.messages = message_list

    """
    Safely sets self.messages to provided set or list messages_set, whose  
    entries are assumed to be of the format 
    (message_time, message_id, message_content)
    """
    def set_messages_set(self, messages_set):
        with self.lock:
            self.messages_set = set(messages_set)

    """
    Safely removes the provided successor from self.successors, if present. 
    Returns True if successor was in self.successors, False otherwise.
    """
    def remove_successor(self, successor):
        with self.lock:
            if successor in self.successors:
                self.successors.remove(successor)
                return True
            return False

    """
    Determines if flush required, if yes then attempts flush to disk, 
    removing flushed messages from self.messages and self.messages_set if 
    successful. Returns True if successful flush, False otherwise.
    """
    def flush_if_needed(self):
        with self.lock: # locked to avoid two threads flushing concurrently
            # if we're already flushing or if we have less than 200 messages in memory, don't flush
            if self.flush_in_progress or len(self.messages) < Node.FLUSH_THRESHOLD: 
                return False
            
            self.flush_in_progress = True # set flag
            to_flush = list(self.messages[:Node.TO_FLUSH]) # to_flush is copy of messages which we will flush
            self.messages = self.messages[Node.TO_FLUSH:] # remove the messages to be flushed from self.messages

        # perform disk write - though unlocked (for performance), this is safe due to flush_in_progress flag check
        try:
            os.makedirs("honeycomb", exist_ok=True) # create directory in user's machine called honeycomb
            path = "honeycomb/messages.jsonl" 
            flush_timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z") # 
            with open(path, "a", buffering=1) as f:
                for (message_time, mid, content) in to_flush:
                    # create JSON for this message
                    rec = {
                        "flush_timestamp": flush_timestamp,
                        "message_time": message_time,
                        "mid": mid,
                        "node": self.url,
                        "content": content
                    }
                    f.write(json.dumps(rec, separators=(",", ":")) + "\n") # appends line to buffer as JSON
                f.flush() # actually writes to disk
        except Exception as e:
            print("Flush write failed:", e)
            with self.lock:
                self.messages = to_flush + self.messages # repair self.messages
                self.flush_in_progress = False
            return False
        
        # flush successful: discard flushed message ids from messages_set
        with self.lock:
            for (_, mid, _) in to_flush:
                self.messages_set.discard(mid)
            self.flush_in_progress = False
        return True
     
    """
    Upon receiving a message in the format 
    {message_id,message_time,node_id,message_content}, parses  message. 
    If we haven't received it before, forwards the message. Also checks if 
    flush needed and, if so, initiates it.
    """
    def receive_message(self, message_str):
        # parse message
        split_message = message_str.split(",", 3)
        message_id = split_message[0]
        message_time = split_message[1]
        node_id = split_message[2]
        message_content = split_message[3]

        print(f"[MSG-RECV] {self.id}, {self.url}  received message from NodeID={node_id}: '{message_content}'")

        forward = False
        need_flush = False

        # only keep the message if we haven't seen it before
        with self.lock:
            if message_id not in self.messages_set:
                forward = True
                self.messages_set.add(message_id) # mark this message as seen
                
                # add it to our message_list
                to_add_message = (message_time, message_id, message_content)
                i = len(self.messages) - 1
                while i >= 0 and self.messages[i][0] > message_time:
                    i -= 1
                self.messages.insert(i + 1, to_add_message)

                # check if we need to flush
                if len(self.messages) >= Node.FLUSH_THRESHOLD and not self.flush_in_progress:
                    need_flush = True

        # forward the message after storing
        if forward:
            self.forward_message(message_str)
        
        # start flushing thread if necessary
        if need_flush:
            flush_thread = threading.Thread(target=self.flush_if_needed, daemon=True)
            flush_thread.start()

    """
    Given message_content, formats message and computes message ID, 
    then passes to receive_message which in turn will forward the message.
    """
    def send_message(self, message_content):
        # form message
        message_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        to_hash = f"{message_content},{message_time},{self.id}"
        message_id = str(self.message_sha_hash(to_hash))
        message_to_add = f"{message_id},{message_time},{self.id},{message_content}"

        # same functionality when sending your own message or receiving a new message
        print(f"[MSG-ORIGIN] Node {self.id}, {self.url} created message: '{message_content}'")
        self.receive_message(message_to_add)

    """
    Given message, probabilistically attempts to forward the message to 
    successors and finger. Probabilities are as follows:
        100% prob of sending to immediate successor
        50% prob of sending to 2nd successor 
        25% prob of sending to 3rd successor 
        12.5% prob of sending to 4th successor
        100% prob of sending to finger.
    """
    def forward_message(self, message):
        successor_list = self.get_successors()

        # forward message to each successor with decreasing probability
        for i in range(len(successor_list)):
            p = 1 / 2**i
            r = random.random()

            if r < p: # forward to this successor
                recipient = successor_list[i]
                if recipient[0] != self.id: # avoid sending XMl-RPC to ourselves
                    for attempt in range(Node.ATTEMPTS):
                        try:
                            recipient_url = recipient[1]
                            # print(f"[MSG-FWD] Node {self.url} → forwarding message to {recipient_url}")
                            successor_xml = rpc_proxy(recipient_url)
                            successor_xml.receive_message(message)
                            break
                        except Exception as e:
                            # print(f"Successor {recipient} not responding, removing from successor list.")
                            if attempt == Node.ATTEMPTS-1: # retry failed
                                self.remove_successor(recipient) # remove failed successor
                                break
                            time.sleep(Node.RETRY_SLEEP)
        
        # forward message to finger with 100% probability - made this way for easy changing
        p = 1 
        r = random.random()

        if r < p: # forward to finger
            finger = self.get_finger()
            if finger is not None:
                
                for attempt2 in range(Node.ATTEMPTS):
                    try:
                        recipient_url = finger[1]
                        # print(f"[MSG-FWD] Node {self.url} → forwarding message to {recipient_url}")
                        finger_xml = rpc_proxy(recipient_url)
                        finger_xml.receive_message(message)
                        break
                    except Exception as e:
                        # print(f"Successor {finger} not responding, setting finger to None")
                        if attempt2 == Node.ATTEMPTS-1: # retry failed
                            removed = self.set_finger(None) # remove failed finger
                            break
                        time.sleep(Node.RETRY_SLEEP)


    """
    Creates new honeycomb network consisting of only this node, with its 
    successor list only containing itself, and its finger and predecessor 
    being None (latter two via init).
    """
    def create(self):
        self.set_successors([(self.id, self.url)])

    """
    Enables a node to join a honeycomb network via an existing node, 
    provided via existing_url. Asks that existing node to find its 
    first successor, which it then sets to be the only element in 
    its successor list.

    Once it obtains a successor the node attempts to retrieve recent 
    messages from its successor.

    When join has completed, a node is guaranteed to either have failed the
    join or to have a successor that is not itself.
    """
    def join(self, existing_url):
        print(f"[JOIN] Node {self.url} joining via node {existing_url}...")

        # prevent self-join
        if existing_url == self.url:
            print(f"Cannot join network using your own URL {self.url}")
            return

        # ask existing node to find who our first successor should be
        for attempt in range(Node.ATTEMPTS):

            try: 
                installed_succ = None
                existing_xml = rpc_proxy(existing_url)
                first_succ = existing_xml.find_first_successor(self.id)

                # avoid isolating if first_succ is ourselves
                # make existing node our successor
                if first_succ[0] == self.id:
                    existing_id = self.id_sha_hash(existing_url)
                    installed_succ = (existing_id, existing_url)
                else:
                    installed_succ = first_succ
                
                self.set_successors([installed_succ])

                # once we join, attempt to get recent messages from first successor
                for attempt2 in range(Node.ATTEMPTS):  # try once + retry once
                    try:
                        # get messages
                        first_succ_xml = rpc_proxy(installed_succ[1])
                        message_list, message_set = first_succ_xml.get_message_info()
                        # update our own messages
                        self.set_messages_list(message_list)
                        self.set_messages_set(message_set)
                        with self.lock:
                            self.join_finished = True
                        return 

                    except Exception as e:
                        if attempt2 == Node.ATTEMPTS-1: # retry failed
                            print(f"Join failed during message retrieval due to {e} after retry.")
                            self.remove_successor(installed_succ)
                            raise RuntimeError(f"message sync failed: {e}")
                        # first try failed: try to retrieve messages again
                        time.sleep(2)

            except Exception as e:
                if attempt == Node.ATTEMPTS-1: # retry failed
                    print(f"Join via {existing_url} failed due to {e} after retry.")
                    os.kill(os.getpid(), signal.SIGTERM)
                    break           
                time.sleep(2)

    """
    Called periodically as part of stabilization to update predecessor. If
    our predecessor is None, or calling node is between our predecessor and 
    us, calling node becomes our predecessor.
    """
    def notify(self, calling_node):
        calling_id, calling_url = calling_node
        predecessor = self.get_predecessor()
        
        # if we don't have a predecessor, calling node becomes our predecessor
        if predecessor is None:
            self.set_predecessor(calling_node)
        
        # if calling node id between our predecessor's id and our id, 
        # it becomes our predecessor
        elif self.between_exclusive(calling_id, predecessor[0], self.id):
            self.set_predecessor(calling_node)

    """
    Pings predecessor to check liveness. If predecessor has failed, sets 
    self.predecessor to None.
    """    
    def check_predecessor(self):
        predecessor = self.get_predecessor()
        if predecessor is not None:
            predecessor_id, predecessor_url = predecessor
            
            # do not allow node to be its own predecessor
            if predecessor_id == self.id:
                self.set_predecessor(None)
            # ping predecessor, remove if dead
            else: 
                for attempt in range(Node.ATTEMPTS):
                    try:
                        predecessor_xml = rpc_proxy(predecessor_url)
                        predecessor_xml.get_successors()
                        break
                    except Exception as e:
                        if attempt == Node.ATTEMPTS-1:
                            self.set_predecessor(None)
                            break
                        # print(f"[PREDECESSOR-FAIL] Node {self.url} detected failed predecessor {predecessor_url}.")
                        time.sleep(Node.RETRY_SLEEP)
    
    """
    Stabilize keeps node's successor lists updated despite churn via three 
    sub-functions. 
        - node checks for a new possible successor by getting it's 
          successor's predecessor
        - node verifies its successor list via reconciling with successor's
          successor list, uses other successors and finger as backup
        - if we updated our first successor, notify that successor of our 
          existence.
    """
    def stabilize(self):
        updated = False

        # STEP 1: CHECK FOR NEW IMMEDIATE SUCCESSOR
        successor_list = self.get_successors()
        if len(successor_list) > 0:
        
            successor = self.get_successors()[0] # grab first successor
            
            # handle transition from 1 node network to 2 node network
            # this is when this node was the creating node
            if successor_list and successor_list[0][0] == self.id:
                pred = self.get_predecessor()
                if pred is not None and pred[0] != self.id:
                    self.set_successors([pred]) # set predecessor to be first successor
            
            # our first successor is not ourselves
            # get successor's predecessor
            else:
                possible_successor = None
                for attempt in range(Node.ATTEMPTS): # try once + retry once
                    try:
                        successor_xml = rpc_proxy(successor[1])
                        possible_successor = successor_xml.get_predecessor()
                        break
                    except Exception as e:
                        if attempt == Node.ATTEMPTS-1:
                            self.remove_successor(successor)
                            possible_successor = None
                            break
                        time.sleep(Node.RETRY_SLEEP)
                
                # we have new a possible successor that we didn't know about
                if possible_successor is not None:
                    possible_successor_id, possible_successor_url = possible_successor

                    # if possible successor ID is between my ID and current successor ID
                    if self.between_exclusive(possible_successor_id, self.id, successor[0]):
                        # this node should be our first successor!
                        # do successor reconciliation w their list
                        updated = self.reconcile(possible_successor)

        # STEP 2: VERIFY SUCCESSOR LIST
        # attempt to reconcile with each of our successors until we do so 
        # successfully or none of our successors respond
        successor_list = self.get_successors()
        i = 0
        while not updated and i < len(successor_list):
            succ = successor_list[i]
            updated = self.reconcile(succ)
            i += 1

        # if all our successors have failed, attempt to reconcile with finger
        finger = self.get_finger()
        if not updated:
            updated = self.reconcile(finger)

        # if our finger failed too, attempt to reconcile with predecessor
        predecessor = self.get_predecessor()
        if not updated:
            updated = self.reconcile(predecessor)

        # if node has finished joining but has become disconnected, kill it
        if self.join_finished and successor_list == [(self.id, self.url)] and finger is None and predecessor is None:
            print("Your node has been disconnected from the network. You can attempt to rejoin.")
            os.kill(os.getpid(), signal.SIGTERM) 
            return       
        # if single node or early join, then its ok and we want notify to run, 
        # otherwise node is disconnected
        # if not updated and not (len(successor_list) == 1 and successor_list[0][0] == self.id):            
        #     print("Node disconnected from network. Please attempt to rejoin.")
        #     return

        # STEP 3: NOTIFY FIRST SUCCESSOR
        successor_list = self.get_successors() # refresh successor_list

        # if all successors removed, there's no one to notify
        if len(successor_list) == 0:
            return
        first_succ = successor_list[0] # update our successor

        if first_succ[0] == self.id: # do not notify ourselves
            return

        # notify our first successor that we may be their predecessor
        for attempt2 in range(Node.ATTEMPTS):
            try:
                first_succ_xml = rpc_proxy(first_succ[1])
                first_succ_xml.notify((self.id, self.url))
                break
            except Exception as e:
                if attempt2 == Node.ATTEMPTS-1:
                    self.remove_successor(first_succ)
                    break
                time.sleep(Node.RETRY_SLEEP)

    """
    Set this node's successor list to be successor followed by n1's successor 
    list minus its last element. Returns True if this is successfully done, 
    False otherwise.

    Note that successor may not actually be this node's successor.
    """
    def reconcile(self, successor):
        if successor is None:
            return False
        successor_id, successor_url = successor

        if successor_id == self.id: # avoid reconciling with yourself
            return False

        # perform reconciliation
        for attempt in range(Node.ATTEMPTS):
            try:
                # get successor's successor list
                successor_xml = rpc_proxy(successor_url)
                successors_succ_list = successor_xml.get_successors()

                new_succ_list = [successor] # this node should be our first successor
                succ_list_members = {successor_id} # keep track of which nodes in our new succ list as we add

                # for each of our successor's successors
                for succ_succ in successors_succ_list:
                    succ_succ_id, succ_succ_url = succ_succ
                    # only add this node to our successor list if
                    #   our new successor list is not full
                    #   this node isn't already in the successor list - avoid duplicates
                    #   this node is not ourselves
                    if len(new_succ_list) < Node.NUM_SUCC and succ_succ_id not in succ_list_members and succ_succ_id != self.id:
                        new_succ_list.append((succ_succ_id, succ_succ_url))
                        succ_list_members.add(succ_succ_id)
                self.set_successors(new_succ_list) # update our successor list with new list
                return True
            except Exception as e:
                if attempt == Node.ATTEMPTS-1:
                    self.remove_successor(successor)
                    return False
                else:
                    time.sleep(Node.RETRY_SLEEP)
    
    """
    Returns True if find_node.id ∈ (my_node.id, successor_node.id] clockwise 
    on the ring.
    """
    def between_inclusive(self, find_node, my_node, successor_node):
        # converting all node id strings to ints for math
        find_node = int(find_node)
        my_node = int(my_node)
        successor_node = int(successor_node)

        # normal case
        if my_node < successor_node:
            return my_node < find_node <= successor_node
        # wrap around case
        else:
            return find_node > my_node or find_node <= successor_node

    """
    Returns True if find_node.id ∈ (my_node.id, successor_node.id) clockwise 
    on the ring.
    """
    def between_exclusive(self, find_node, my_node, successor_node):
        # converting all node id string to int
        find_node = int(find_node)
        my_node = int(my_node)
        successor_node = int(successor_node)
        # normal case
        if my_node < successor_node:
            return my_node < find_node < successor_node
        else:
            # wrap around case
            return find_node > my_node or find_node < successor_node

    """
    Periodically called, maintains correctness of finger by setting finger 
    to the closest node whose id is greater than or equal to the id that is 
    located halfway across the ring.
    """
    def fix_fingers(self):
        offset = 2 ** (Node.M_BIT - 1) # offset for id halfway across the ring 
        target = (int(self.id) + offset) % (2 ** Node.M_BIT) # mod ensures wrap around
        finger = self.find_first_successor(str(target))
        if finger[0] == self.id:
            finger = None
        self.set_finger(finger) # update finger

    """
    Returns the appropriate successor to node_id. If our successor should be 
    node_id's successor, we return this directly. Otherwise, we forward this 
    call to our first successor, and other successors or finger in case of 
    failure. In special cases use finger first as speedup. Upon total 
    failure return ourselves.
    """
    def find_first_successor(self, node_id):
        # speed up: use the finger when target is in the far half
        finger = self.get_finger() # finger is (id, url) or None
        if finger is not None:
            N = 1 << Node.M_BIT
            # convert str to int for math
            nid = int(node_id)
            sid = int(self.id)
            fid = int(finger[0])
            # if node.id is past our finger in the id space, but before us
            if ((nid - sid) % N) > ((fid - sid) % N):
                for attempt in range(Node.ATTEMPTS):
                    try:
                        finger_xml = rpc_proxy(finger[1])
                        return finger_xml.find_first_successor(node_id)
                    except Exception as e:
                        if attempt == Node.ATTEMPTS-1:
                            # print(f"Finger {finger} not responding, set to None."))
                            self.set_finger(None)
                            break
                        time.sleep(Node.RETRY_SLEEP)

        # either speed up doesn't apply or didn't work -> use successors
        successor_list = self.get_successors()
        if len(successor_list) > 0: 
            first_successor = successor_list[0]
        
            # if our first successor is ourselves, we have no one to forward to
            # just return ourselves
            if first_successor[0] == self.id: 
                return (self.id, self.url)
            
            # if node_id is between ourself and our first successor, 
            # return our first successor 
            first_succ_id = first_successor[0]
            if self.between_inclusive(node_id, self.id, first_succ_id): 
                return first_successor

        # otherwise we need to forward call to our successor
        # attempt to forward to successor, fallback to next one in case of failure
        i = 0
        while i < len(successor_list):
            successor = successor_list[i]
            succ_url = successor[1]

            for attempt2 in range(Node.ATTEMPTS):
                try: # attempt to forward call to this successor
                    successor_xml = rpc_proxy(succ_url)
                    return successor_xml.find_first_successor(node_id)
                except Exception as e:
                    i += 1
                    if attempt2 == Node.ATTEMPTS-1:
                        # print(f"Successor {successor} not responding, removing from successor list.")
                        self.remove_successor(successor)
                        break
                    time.sleep(Node.RETRY_SLEEP)
                    
        
        # use finger as backup in case successors fail
        # even if we attempted speedup and it failed, it's ok to retry bc finger may have updated
        finger = self.get_finger() 
        if finger is not None:
            finger_url = finger[1]
            for attempt3 in range(Node.ATTEMPTS):
                try:
                    finger_xml = rpc_proxy(finger[1])
                    return finger_xml.find_first_successor(node_id)
                except Exception as e:
                    if attempt3 == Node.ATTEMPTS-1:
                        # print(f"Finger {finger} not responding, set to None."))
                        self.set_finger(None)
                        break
                    time.sleep(Node.RETRY_SLEEP)

        # as last resort we return ourselves
        return (self.id, self.url)

    """
    Generates ring in list form by recursively obtaining first successor of 
    each node.
    """
    def get_ring(self):
        ring = []
        seen = set()

        # start with self
        start_id = self.id
        start_url = self.url
        ring.append((start_id, start_url))
        seen.add(start_id)
        successor_list = self.get_successors()

        if len(successor_list) > 0:
            current_id, current_url = self.get_successors()[0]

            while current_id not in seen:
                ring.append((current_id, current_url))
                seen.add(current_id)
                try:
                    proxy = rpc_proxy(current_url)
                    current_id, current_url = proxy.get_successors()[0]
                except:
                    break
        return ring

    """
    Gets region of machine that this node is running on.
    """
    def get_region(self, url):
        ip = url.split(":")[0]
        return REGION.get(ip, "Unknown Region")

    """
    Prints nice visual of ring generated from get_ring.
    """
    def print_ring_flowchart(self):
        ring = self.get_ring()

        # Use full node ID
        def sid(x): return x
        def region(url): return REGION.get(url.split(":")[0], "Unknown")

        # Build the 3-line block for each node
        labels = []
        for node_id, url in ring:
            node_line = ("*" if node_id == self.id else "") + sid(node_id)
            ip_line = url
            region_line = f"({region(url)})"
            labels.append([node_line, ip_line, region_line])

        # Compute width of widest line and add padding
        max_width = max(len(line) for block in labels for line in block)
        max_width += 20   # 10 chars of buffer left + right

        def make_box(block):
            top =  "┌" + "─" * max_width + "┐"
            mid =  "│ " + block[0].center(max_width - 2) + " │"
            mid2 = "│ " + block[1].center(max_width - 2) + " │"
            mid3 = "│ " + block[2].center(max_width - 2) + " │"
            bot =  "└" + "─" * max_width + "┘"
            return [top, mid, mid2, mid3, bot]

        print("\n==================== RING TOPOLOGY ====================\n")
        for i, block in enumerate(labels):
            box = make_box(block)
            for line in box:
                print(" " * 6 + line)

            if i != len(labels) - 1:
                print(" " * (6 + max_width // 2) + "   ↓")
                print()

        print(" " * (6 + max_width // 2) + "   ↓")
        print(" " * (6 + max_width // 2) + " (back to start)\n")
        print(f"There are {len(ring)} nodes in this ring")
        print("========================================================\n")


"""
Runs honeycomb node via CLI. User may broadcast message, view messages in 
memory, see the node's id, successor list, finger, and predecessor, see 
the ring's topology, and exit the network.
"""
if __name__ == "__main__":
    # Accept either:
    #   python3 node.py <ip> <port>  (for first joining)
    #   python3 node.py <ip> <port> <existing_ip> <existing_port> 
    if len(sys.argv) not in (3, 5):
        print("Usage:")
        print("  python3 node.py <ip> <port>")
        print("  python3 node.py <ip> <port> <existing_ip> <existing_port>")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])
    node = Node(ip, port)

    if len(sys.argv) == 5:
        existing_ip = sys.argv[3]
        existing_port = sys.argv[4]
        existing_url = f"{existing_ip}:{existing_port}"

        print(f"[Node {port}] starting... attempting to join ring via {existing_url}...")
        node.join(existing_url)
    else:
        print(f"[Node {port}] starting as first node in ring...")
        node.create()  # successor = self

    # starting the node
    server_thread = threading.Thread(target=node.start_node, daemon=True)
    server_thread.start()

    # background thread for new messages 
    def new_message_watcher():
        last_seen = 0
        while True:
            msgs = node.get_messages()
            if len(msgs) > last_seen:
                new = msgs[last_seen:]
                for t, mid, message in new:
                    sys.stdout.write(
                        f"\n New message received at {t}: {message}\n"
                        f"[Node {port}] > "
                    )
                    sys.stdout.flush()
                last_seen = len(msgs)
            time.sleep(1)

    watcher_thread = threading.Thread(target=new_message_watcher, daemon=True)
    watcher_thread.start()

    print("\n======================================================")
    print("          DISASTER COMMUNICATION SYSTEM")
    print("======================================================")
    print(
        "You are now connected to a peer-to-peer emergency communication network.\n"
        "This system allows you to send short, text based broadcast messages to\n"
        "others in your network. These messages may include:\n"
        "  • Requests for help or supplies\n"
        "  • Information about local conditions or hazards\n\n"
        "Use this terminal to send messages and view incoming messages\n"
        "from others in the network.\n"
    )
    print("Available Commands:")
    print("  send <message>   → broadcast a message to everyone else")
    print("  list             → view stored messages")
    print("  info             → show node ID, successor, and predecessor")
    print("  ring             → visualize the ring topology")
    print("  exit             → close the node")
    print("======================================================\n")

    while True:
        try:
            user_input = input(f"[Node {port}] > ").strip()

            if user_input == "":
                continue

            if user_input.startswith("send "):
                msg = user_input[5:].strip()
                if len(msg) == 0:
                    print("Message cannot be empty.")
                    continue
                node.send_message(msg)
                continue

            elif user_input == "list":
                msgs = node.get_messages()
                if not msgs:
                    print("(no messages stored)")
                else:
                    print("\n--- Messages Received ---")
                    for t, mid, message in msgs:
                        print(f"{t} : {message}")
                    print("--------------------------\n")
                continue

            elif user_input == "info":
                print(f"Node ID: {node.id}")
                print(f"Successor list: {node.get_successors()}")
                print(f"Predecessor: {node.get_predecessor()}")
                print(f"Finger: {node.get_finger()}")
                continue

            elif user_input == "ring":
                time.sleep(0.1)
                node.print_ring_flowchart()

            elif user_input == "exit":
                print("Shutting down node.")
                sys.exit(0)

            else:
                print("Unknown command. Valid commands: send, list, info, exit, ring")

        except KeyboardInterrupt:
            print("\nKeyboard interrupt received. Exiting.")
            sys.exit(0)
