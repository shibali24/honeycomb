# HONEYCOMB

**Honeycomb** is a lightweight, decentralized peer-to-peer (P2P) communication platform designed for disaster situations where traditional infrastructure is unreliable, congested, or down. Instead of relying on a central server, Honeycomb runs as a self-healing overlay network where every participant is both a client and a relay, enabling short text broadcasts to propagate even under churn and node failures.

## Docs (PDF)

- **Final paper:** [final_paper.pdf](final_paper.pdf)  
- **Presentation slides:** [honeycomb_pres_slides.pdf](honeycomb_pres_slides.pdf)

---

## Key Ideas

### Chord-inspired ring (lightweight routing state)
Nodes form a logical ring using consistent hashing (SHA-1 of IP:port mapped into an m-bit ID space). Each node maintains small routing state to keep overhead low while remaining resilient:
- **Predecessor**
- **Successor list** (multiple immediate clockwise neighbors for fallback)
- **Finger** (a long-range contact used to speed routing/placement without a full finger table)

### Fault-tolerant broadcast
Honeycomb focuses on **broadcast dissemination** (not a full DHT). Messages are forwarded along redundant paths:
- Deterministic forwarding to key neighbors (e.g., first successor + finger)
- Probabilistic forwarding to additional successors with decreasing probability to increase robustness under failures while limiting traffic
- **Deduplication** via a message ID set so the network doesn’t explode with repeats

### Concurrency + availability
Each node runs a multi-threaded XML-RPC server plus background maintenance threads so it can:
- receive/forward messages promptly
- repair ring pointers continuously (stabilize/check_predecessor/fix_finger)
- update the CLI without blocking network behavior

---

## Repository Structure

- `node.py` — main Honeycomb node implementation  
- `experimental_script/` — scripts used to run Experiment 1 & 2  
- `early_testing/` — early prototypes / tests  
- `final_paper.pdf` — final write-up  
- `honeycomb_pres_slides.pdf` — presentation deck  
- `*.zip` — experiment logs / run artifacts (if included)

---

## Running Honeycomb (create + join)

Honeycomb uses a simple bootstrap workflow:

- **One node creates the ring** (no join target)
- **Any other node joins** by providing the IP:port of a node already in the ring

### 1) Create the ring (first node)

Run this on the machine that will start the network:

```bash
python3 node.py <THIS_NODE_IP> <PORT>
```

### Example:

```bash
python3 node.py 12.161.241.238 8519
```

### 2) Join the ring (any additional node)

Run this on a different machine to join an existing ring:

```bash
python3 node.py <THIS_NODE_IP> <PORT> <BOOTSTRAP_NODE_IP> <BOOTSTRAP_PORT>
```

### Example (joining via the first node):

```bash
python3 node.py 40.192.121.231 8519 15.161.241.238 8519
```

### Notes

<THIS_NODE_IP> should be the address other nodes can reach 

<BOOTSTRAP_NODE_IP>:<BOOTSTRAP_PORT> can be any node already running in the ring (not necessarily the first).

### How It Works 

- Create a network: one node initializes the ring.
- Join: other nodes join by contacting a known node, finding their correct successor, then integrating via periodic stabilization.
- Maintenance: nodes continuously repair successor/predecessor/finger pointers in the background.
- Broadcast: messages propagate across the ring through redundant forwarding + deduplication.

### Experiments
Honeycomb includes experiments that evaluate behavior under failures and load, including:

- Network recovery under node failures: convergence time as failures increase
- Delivery reliability under traffic + concurrent node failures: message delivery fraction/accuracy under progressive node loss

See the paper and slides for full methodology and results!

