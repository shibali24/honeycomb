import subprocess
import time
import xmlrpc.client
import signal
import sys
import threading

nodes = [
    ("9009", "localhost", "9010"),
    ("9010", "localhost", "9011"),
    ("9011", "localhost", "9009")
]

procs = []

def cleanup_and_exit():
    print("\n[Main] Caught Ctrl+C — shutting down all nodes...")
    for p in procs:
        try:
            p.terminate()
        except:
            pass

    time.sleep(0.5)

    for p in procs:
        try:
            p.kill()
        except:
            pass

    print("[Main] All nodes terminated. Exiting.")
    sys.exit(0)

signal.signal(signal.SIGINT, lambda sig, frame: cleanup_and_exit())

# ------------------------------------------------------------
# START NODES
# ------------------------------------------------------------
for port, succ_ip, succ_port in nodes:
    print(f"Launching Node {port} → {succ_ip}:{succ_port}")
    
    # pass each argument as a string
    p = subprocess.Popen(["python3", "node.py", port, succ_ip, succ_port])
    procs.append(p)
    time.sleep(0.3)

print("Nodes running. Waiting for initialization...")
time.sleep(1.5)

# ------------------------------------------------------------
# CONCURRENT MESSAGE SEND TEST
# ------------------------------------------------------------
def send_concurrent(i):
    try:
        proxy = xmlrpc.client.ServerProxy("http://localhost:9009")
        proxy.send_message(f"Concurrent_Message_{i}")
        print(f"[Thread {i}] sent")
    except Exception as e:
        print(f"[Thread {i}] ERROR:", e)

NUM_THREADS = 30

print(f"\n[Main] Launching {NUM_THREADS} concurrent sends...")
threads = []

for i in range(NUM_THREADS):
    t = threading.Thread(target=send_concurrent, args=(i,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print("\n[Main] All concurrent sends finished.")
time.sleep(2)

print("\n[Main] Fetching messages from Node 9009...")
try:
    proxy = xmlrpc.client.ServerProxy("http://localhost:9009")
    msgs = proxy.get_messages()
    print(f"[Main] Node 9009 stored {len(msgs)} messages.")
    for m in msgs:
        print("   ", m)
except Exception as e:
    print("Could not fetch messages:", e)

print("\nNodes running — press Ctrl+C to stop.")

try:
    signal.pause()
except KeyboardInterrupt:
    cleanup_and_exit()
