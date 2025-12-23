import subprocess
import time
import xmlrpc.client
import random
import signal
import sys

# ===========================================
# CLEANUP HANDLING
# ===========================================
PROCS = []

def cleanup():
    print("\n[TEST] Shutting down...")
    for p in PROCS:
        try: p.terminate()
        except: pass

    time.sleep(0.3)

    for p in PROCS:
        try: p.kill()
        except: pass

    print("[TEST] All nodes terminated.\n")

def handle_sigint(sig, frame):
    cleanup()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_sigint)

# ===========================================
# UTILS
# ===========================================
def rpc(port):
    return xmlrpc.client.ServerProxy(f"http://localhost:{port}")

# ===========================================
# TEST CONFIG
# ===========================================
BASE_PORT = 9000
NUM_NODES = 4  # A, B, C, D
IP = "localhost"

# ===========================================
# START NODE A — CREATE NEW RING
# ===========================================
print("[TEST] Launching Node A (create new ring)...")
portA = BASE_PORT
pA = subprocess.Popen(["python3", "node.py", str(portA), IP, str(portA)])
PROCS.append(pA)

time.sleep(1)

# Force node A into create() mode
rpc(portA).create()

print(f"  Node A running at {IP}:{portA}")
time.sleep(0.5)

# ===========================================
# START OTHER NODES AND JOIN RING
# ===========================================
for i in range(1, NUM_NODES):
    port = BASE_PORT + i
    print(f"[TEST] Launching Node {chr(ord('A') + i)} on port {port} (join ring via A)...")

    p = subprocess.Popen(
        ["python3", "node.py", str(port), IP, str(portA)]  # temporary successor
    )
    PROCS.append(p)
    time.sleep(1)

    # actual join operation
    rpc(port).join(f"{IP}:{portA}")
    print(f"  Node {chr(ord('A') + i)} joined via Node A")

time.sleep(20)

# ===========================================
# PRINT SUCCESSORS
# ===========================================
print("\n[TEST] Successor Table:")
for i in range(NUM_NODES):
    port = BASE_PORT + i
    try:
        succ = rpc(port).get_successor()
        print(f"  Node {chr(ord('A') + i)} ({port}) → successor: {succ}")
    except Exception as e:
        print(f"  Node {chr(ord('A') + i)} ERROR: {e}")

# ===========================================
# RANDOM SUCCESSOR QUERIES
# ===========================================
print("\n[TEST] Random find_successor() queries:")
for _ in range(10):
    x = random.randint(0, 2**32 - 1)
    try:
        result = rpc(portA).find_successor(str(x))
        print(f"  find_successor({x}) → {result}")
    except Exception as e:
        print(f"  ERROR calling find_successor({x}): {e}")

print("\n[TEST COMPLETE] Press Ctrl+C to shut everything down.")

try:
    signal.pause()
except KeyboardInterrupt:
    cleanup_and_exit()
