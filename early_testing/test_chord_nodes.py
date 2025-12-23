import subprocess
import time
import xmlrpc.client
import threading
import signal
import sys

# ===========================================
# NODE DEFINITIONS (port, succ_ip, succ_port)
# ===========================================
NODE_CONFIG = [
    ("9009", "localhost", "9010"),
    ("9010", "localhost", "9011"),
    ("9011", "localhost", "9009"),
]

PROCS = []

# ===========================================
# CLEANUP HANDLER
# ===========================================
def cleanup():
    print("\n[TEST] Shutting down nodes...")
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
# START NODES
# ===========================================
print("[TEST] Launching nodes...")
for port, sip, sport in NODE_CONFIG:
    print(f"  → Node {port} with successor {sip}:{sport}")
    p = subprocess.Popen(["python3", "node.py", port, sip, sport])
    PROCS.append(p)
    time.sleep(0.2)

print("[TEST] Waiting for nodes to initialize...")
time.sleep(1.5)

# ===========================================
# HELPER: RPC PROXY
# ===========================================
def rpc(port):
    return xmlrpc.client.ServerProxy(f"http://localhost:{port}")

# ===========================================
# TEST 1 — FIND SUCCESSOR() TEST
# ===========================================
def ring_test_find_successor():
    print("\n[TEST 1] Testing find_successor()\n")

    # pick some IDs on the ring
    test_ids = ["0", "1000", "40000", str(2**31), str(2**32 - 10)]

    for tid in test_ids:
        try:
            result = rpc("9009").find_successor(tid)
            print(f"  find_successor({tid}) → {result}")
        except Exception as e:
            print(f"  ERROR calling find_successor({tid}): {e}")

# ===========================================
# TEST 2 — CONCURRENT SEND MESSAGE
# ===========================================
def ring_test_concurrent_msgs(num=20):
    print(f"\n[TEST 2] Sending {num} concurrent messages...\n")

    def send(i):
        try:
            rpc("9009").send_message(f"Concurrent_{i}")
            print(f"[send-{i}] OK")
        except Exception as e:
            print(f"[send-{i}] ERROR: {e}")

    threads = []
    for i in range(num):
        t = threading.Thread(target=send, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n[TEST 2] All sends complete.\n")

# ===========================================
# TEST 3 — FETCH MESSAGES
# ===========================================
def ring_test_fetch():
    print("[TEST 3] Fetching stored messages from Node 9009...\n")
    try:
        out = rpc("9009").get_messages()
        print(f"Node 9009 has {len(out)} messages:\n")
        for m in out:
            print("   ", m)
    except Exception as e:
        print("FAILED to fetch messages:", e)

# ===========================================
# RUN TESTS
# ===========================================
try:
    ring_test_find_successor()
    ring_test_concurrent_msgs(30)
    time.sleep(1)
    ring_test_fetch()

    print("\n[TEST] All tests completed successfully!")
    print("[TEST] Press Ctrl+C to shut down nodes.\n")

try:
    signal.pause()
except KeyboardInterrupt:
    cleanup_and_exit()
