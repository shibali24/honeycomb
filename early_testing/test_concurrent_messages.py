import subprocess
import time
import xmlrpc.client
import random
import signal
import sys
import threading

# ===========================================
# CLEANUP HANDLING
# ===========================================
PROCS = []

def cleanup():
    print("\n[TEST] Shutting down...")
    for p in PROCS:
        try:
            p.terminate()
        except:
            pass

    time.sleep(0.3)

    for p in PROCS:
        try:
            p.kill()
        except:
            pass

    print("[TEST] All nodes terminated.\n")

def handle_sigint(sig, frame):
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_sigint)

# ===========================================
# UTILS
# ===========================================
def rpc(port):
    return xmlrpc.client.ServerProxy(f"http://localhost:{port}", allow_none=True)

# ===========================================
# TEST CONFIG
# ===========================================
BASE_PORT = 9000
NUM_NODES = 4        # A, B, C, D
IP = "localhost"
MESSAGES_PER_NODE = 5

# ===========================================
# START NODE A — CREATE NEW RING
# ===========================================
print("[TEST] Launching Node A (create new ring)...")
portA = BASE_PORT
pA = subprocess.Popen(["python3", "node.py", str(portA), IP, str(portA)])
PROCS.append(pA)

# Give it time to start XML-RPC server
time.sleep(1.5)

# Force node A into create() mode (successor = itself)
print("[TEST] Calling create() on Node A...")
rpc(portA).create()
print(f"  Node A running at {IP}:{portA}")
time.sleep(0.5)

# ===========================================
# START OTHER NODES AND JOIN RING
# ===========================================
for i in range(1, NUM_NODES):
    port = BASE_PORT + i
    name = chr(ord("A") + i)
    print(f"[TEST] Launching Node {name} on port {port} (join ring via A)...")

    # start node process with temporary successor (A)
    p = subprocess.Popen(
        ["python3", "node.py", str(port), IP, str(portA)]
    )
    PROCS.append(p)

    time.sleep(1.5)  # give node time to start and register XML-RPC

    # actual join operation into the ring
    print(f"[TEST] Node {name} joining via Node A...")
    rpc(port).join(f"{IP}:{portA}")
    print(f"  Node {name} ({IP}:{port}) joined ring.")

# ===========================================
# LET RING STABILIZE
# ===========================================
print("\n[TEST] Waiting for ring to stabilize (stabilize() running in background)...")
time.sleep(20)

# ===========================================
# PRINT SUCCESSOR TABLE
# ===========================================
print("\n[TEST] Successor Table:")
for i in range(NUM_NODES):
    port = BASE_PORT + i
    name = chr(ord("A") + i)
    try:
        succ = rpc(port).get_successor()
        print(f"  Node {name} ({port}) → successor: {succ}")
    except Exception as e:
        print(f"  Node {name} ({port}) ERROR: {e}")

# ===========================================
# CONCURRENT MESSAGE SENDING
# ===========================================
print("\n[TEST] Starting concurrent message sending from all nodes...\n")

def sender_thread(port, label):
    proxy = rpc(port)
    for k in range(MESSAGES_PER_NODE):
        msg = f"{label} message #{k}"
        try:
            print(f"[SEND] {label} on port {port} → '{msg}'")
            proxy.send_message(msg)
        except Exception as e:
            print(f"[ERROR] sending from {label} ({port}): {e}")
        time.sleep(random.uniform(0.2, 0.8))  # stagger sends a bit

threads = []
for i in range(NUM_NODES):
    port = BASE_PORT + i
    name = f"Node-{chr(ord('A') + i)}"
    t = threading.Thread(target=sender_thread, args=(port, name), daemon=True)
    threads.append(t)
    t.start()

# Wait for all message-sender threads to finish
for t in threads:
    t.join()

print("\n[TEST] All concurrent sends finished. Let messages propagate around the ring...")
time.sleep(5)

# ===========================================
# PRINT STORED MESSAGES ON EACH NODE
# ===========================================
print("\n[TEST] Messages stored at each node (sorted by timestamp):")
for i in range(NUM_NODES):
    port = BASE_PORT + i
    name = chr(ord("A") + i)
    try:
        msgs = rpc(port).get_messages()
        print(f"\n  Node {name} ({port}) has {len(msgs)} messages:")
        for ts, content in msgs:
            print(f"    [{ts}] {content}")
    except Exception as e:
        print(f"  Node {name} ({port}) ERROR reading messages: {e}")

print("\n[TEST COMPLETE] Press Ctrl+C to shut everything down.")

# Block until Ctrl+C
try:
    signal.pause()
except KeyboardInterrupt:
    pass
finally:
    cleanup()
