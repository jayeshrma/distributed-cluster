# Distributed Key-Value Service: Fault Tolerance, Replication, and Consistency

A fault-tolerant, quorum-replicated, and strongly consistent **Distributed Key-Value Storage Service** implemented in Python. This project is modeled after the foundational distributed systems principles taught in **MIT 6.5840 / 6.824** (Primary-Backup Replication, Raft Majority Quorum Consensus, and At-Most-Once Linearizable RPC Semantics).

## 1. System Architecture & Core Guarantees

This distributed service runs a 3-node cluster (`127.0.0.1:8001`, `127.0.0.1:8002`, `127.0.0.1:8003`) that satisfies the three fundamental pillars of distributed systems engineering:

### 1.1 Replication (Majority Quorum State Machine)

* **Single-Leader Serialization:** All client mutations (`PUT` and `APPEND`) are serialized through a single active **Leader** node to prevent split-brain write conflicts.
* **Quorum Broadcast:** When the Leader receives a write request, it increments its logical clock (`commit_version`), updates its state machine, and broadcasts the state update to all **Backup** replicas via HTTP RPC (`POST /internal/replicate`).
* **Majority Commit Rule:** A write operation is marked `COMMITTED` and returned to the client if and only if a **majority quorum** ($\lfloor N/2 \rfloor + 1 = 2 \text{ out of } 3 \text{ nodes}$) acknowledges and persists the update.

### 1.2 Consistency (Linearizability & At-Most-Once Deduplication)

* **Monotonic Version Ordering:** Every state mutation is tagged with a strictly increasing `commit_version` ($v_1, v_2, \dots, v_n$). Replicas only apply incoming state updates if the incoming version is greater than or equal to their local `commit_version`, guaranteeing that all nodes converge on the exact same state.
* **At-Most-Once RPC Semantics:** Unreliable networks can cause clients to retry requests when an ACK is delayed or dropped. To prevent non-idempotent operations (such as `APPEND`) from executing twice, every node maintains a persistent `dedup_table` mapping `client_id -> last_request_id`. Duplicate requests with an already-executed `req_id` are intercepted and immediately return the cached outcome (`COMMITTED (DEDUP)`).
* **Transparent Follower Forwarding:** If a client sends a write operation to a Backup replica (`Node 2` or `Node 3`), the Backup transparently proxies the request to the active Leader, preserving linearizable execution order across the cluster.

### 1.3 Fault Tolerance (Automated Failover, Persistence & Crash Recovery)

* **Heartbeat Failure Detection & Leader Election:** Every node runs a background daemon (`heartbeat_and_election_loop`) that polls peer health (`GET /health`) every $1.0\text{ s}$. If the active Leader crashes or becomes unreachable, surviving nodes detect the timeout and automatically elect the lowest alive Node ID as the new Leader.
* **Atomic Disk Persistence:** On every committed write, each replica writes its `commit_version`, `store`, and `dedup_table` atomically to a local state file (`node_X_state.json` via temporary file swap), ensuring durability across full machine reboots.
* **Anti-Entropy State Synchronization:** When a crashed node restarts and rejoins the cluster, it compares its local `commit_version` against active peers during heartbeat checks. If it lags behind, it automatically fetches and applies the latest state snapshot from an up-to-date peer via `GET /internal/sync`.

## 2. Repository Structure

```text
distributed-cluster/
├── .gitignore               # Excludes runtime state files and cache
├── README.md                # Architecture, instructions, test logs, and screenshots
├── node.py                  # Core multi-threaded distributed KV server node
├── test_cluster.py          # Automated verification & consistency test suite
├── dashboard.html           # Real-time visual cluster monitor & interactive client
└── screenshots/             # Verification screenshots
    ├── 1.png                # Healthy 3-node cluster & initial leader election
    ├── 2.png                # Automated replication & deduplication test results
    ├── 3.png                # Fault tolerance: Leader crash & automatic failover
    └── 4.png                # Node recovery & anti-entropy state synchronization
```

## 3. Execution Instructions

### Prerequisites

* **Python 3.8+** (Uses Python's built-in standard library modules `http.server`, `urllib.request`, `threading`, and `json`. Zero external `pip` packages are required.)

### Step 1: Start the 3-Node Distributed Cluster

Open **three separate terminal windows** inside the project directory and start one node in each terminal:

**Terminal 1 (Node 1 — Port 8001):**
```bash
python node.py 1
```

**Terminal 2 (Node 2 — Port 8002):**
```bash
python node.py 2
```

**Terminal 3 (Node 3 — Port 8003):**
```bash
python node.py 3
```

### Step 2: Run the Automated Test Suite

Open a **fourth terminal window** in the project directory and run the verification script:
```bash
python test_cluster.py
```

### Step 3: Launch the Real-Time Visual Dashboard

Double-click `dashboard.html` to open it in any web browser. Use the control bar at the top of the page to:
1. Submit `PUT` and `APPEND` operations to any node in the cluster.
2. Click **Simulate Duplicate Retry** to verify at-most-once request deduplication (`COMMITTED (DEDUP)`).
3. Observe live node roles (`LEADER`, `BACKUP`, `CRASHED / OFFLINE`), commit versions, and replicated JSON state across all 3 servers.

### Step 4: Test Fault Tolerance & Recovery Manually

1. In **Terminal 1**, press `Ctrl + C` to terminate **Node 1** (the current Leader).
2. Observe **Terminal 2**, **Terminal 3**, and `dashboard.html` automatically detect the failure within 1 second and promote **Node 2** to `LEADER`.
3. Submit a new write (`grade = A+`) via `dashboard.html` targeted at **Node 2 (:8002)** and verify that the cluster commits the write (`v4`) using a $2/3$ majority quorum.
4. Restart **Node 1** in Terminal 1 (`python node.py 1`) and observe it reload its disk state (`v3`) and immediately synchronize the missed write (`v4`) from Node 2.

## 4. API Reference

| Endpoint | Method | Role | Description |
| :--- | :--- | :--- | :--- |
| `/kv` | `POST` | Client API | Submits a `PUT` or `APPEND` operation with `key`, `value`, `op`, `client_id`, and `req_id`. |
| `/kv?key=<k>` | `GET` | Client API | Reads the current value, version, and serving node role for `<k>`. |
| `/health` | `GET` | Internal / UI | Returns node health, current role (`LEADER`/`BACKUP`), `commit_version`, and store snapshot. |
| `/internal/replicate` | `POST` | Internal RPC | Used by the Leader to broadcast committed mutations to Backup replicas. |
| `/internal/sync` | `GET` | Internal RPC | Used by recovering nodes to pull the latest state machine and deduplication table. |

## 5. Testing Results & Execution Logs

### 5.1 Automated Verification Suite Output (`python test_cluster.py`)

The automated test script validates three critical distributed properties:
1. **Test 1 (Quorum Replication & Linearizable Reads):** Confirms that a `PUT` operation committed on the Leader is immediately readable with identical version numbers (`v1`) across all three replicas.
2. **Test 2 (At-Most-Once Deduplication):** Sends an `APPEND` operation (`+Lab1`) twice with the exact same `(client_id='cli-1', req_id=102)`. Confirms the first executes (`COMMITTED`) and the retry is safely filtered (`COMMITTED (DEDUP)`), resulting in `"MIT-6.5840+Lab1"` rather than `"MIT-6.5840+Lab1+Lab1"`.
3. **Test 3 (Follower-to-Leader Forwarding):** Sends a `PUT` request directly to Backup Node 3 and verifies it is transparently forwarded to Leader Node 1, replicated across the quorum, and committed at `v3`.

```text
=== TEST 1: Quorum Replication & Strong Consistency ===
Leader Write Result: {'status': 'COMMITTED', 'leader': 1, 'version': 1, 'acks': 3, 'value': 'MIT-6.5840'}
  -> Node 1 Read: value='MIT-6.5840', version=1, role=LEADER
  -> Node 2 Read: value='MIT-6.5840', version=1, role=BACKUP
  -> Node 3 Read: value='MIT-6.5840', version=1, role=BACKUP

=== TEST 2: At-Most-Once Linearizable Deduplication ===
Sending APPEND '+Lab1' twice with identical (client_id='cli-1', req_id=102)...
  -> First Attempt : COMMITTED | Value: MIT-6.5840+Lab1
  -> Retry Attempt : COMMITTED (DEDUP) | Value: MIT-6.5840+Lab1

=== TEST 3: Follower-to-Leader Write Forwarding ===
  -> Write sent to Backup Node 3 committed by Leader Node 1: {'status': 'COMMITTED', 'leader': 1, 'version': 3, 'acks': 3, 'value': 'Linearizable'}

[SUCCESS] ALL AUTOMATED CONSISTENCY, DEDUPLICATION & REPLICATION TESTS PASSED!
```

### 5.2 Cluster Node Terminal Logs

**Node 1 (Initial Leader) Output:**
```text
=== Node 1 started at http://127.0.0.1:8001 (Initial Leader: Node 1) ===
[Node 1] QUORUM COMMITTED PUT(course=MIT-6.5840) | v1 | ACKs: 3/3
[Node 1] QUORUM COMMITTED APPEND(course=MIT-6.5840+Lab1) | v2 | ACKs: 3/3
[Node 1] DEDUP: Ignored duplicate request (cli-1, 102)
[Node 1] QUORUM COMMITTED PUT(consistency=Linearizable) | v3 | ACKs: 3/3
```

**Node 2 (Backup -> Elected Leader on Failover) Output:**
```text
=== Node 2 started at http://127.0.0.1:8002 (Initial Leader: Node 1) ===
[Node 2] REPLICATED key='course' val='MIT-6.5840' (v1)
[Node 2] REPLICATED key='course' val='MIT-6.5840+Lab1' (v2)
[Node 2] REPLICATED key='consistency' val='Linearizable' (v3)
[Node 2] ELECTION: Failover triggered! New Leader is Node 2
[Node 2] QUORUM COMMITTED PUT(grade=A+) | v4 | ACKs: 2/3
```

**Node 3 (Backup Replica) Output:**
```text
=== Node 3 started at http://127.0.0.1:8003 (Initial Leader: Node 1) ===
[Node 3] REPLICATED key='course' val='MIT-6.5840' (v1)
[Node 3] REPLICATED key='course' val='MIT-6.5840+Lab1' (v2)
[Node 3] Forwarding PUT(consistency) to Leader Node 1
[Node 3] REPLICATED key='consistency' val='Linearizable' (v3)
[Node 3] ELECTION: Failover triggered! New Leader is Node 2
[Node 3] REPLICATED key='grade' val='A+' (v4)
```

**Node 1 Crash Recovery Output (Upon Restart):**
```text
[Node 1] Loaded persisted state from disk (v3)
=== Node 1 started at http://127.0.0.1:8001 (Initial Leader: Node 1) ===
[Node 1] RECOVERED & SYNCED state to v4 from Node 2
```

## 6. Verification Screenshots

### 6.1 Healthy Cluster Startup & Initial Leader Election
*All three nodes are active. Node 1 is elected as `LEADER` (`:8001`), while Nodes 2 and 3 operate as synchronized `BACKUP` replicas (`:8002`, `:8003`).*

![Healthy Cluster Startup and Initial Leader Election](screenshots/1.png)

---

### 6.2 Automated Replication, Linearizability & Deduplication Tests
*Execution of `test_cluster.py` verifying quorum replication, duplicate request filtering (`req_id=102`), and follower write forwarding across all 3 nodes (`v3`).*

![Automated Replication and Deduplication Tests](screenshots/2.png)

---

### 6.3 Fault Tolerance: Leader Crash & Automatic Failover
*Node 1 is terminated (`CRASHED / OFFLINE`). Surviving nodes detect the heartbeat timeout, automatically elect Node 2 as the new `LEADER`, and commit `grade = A+` (`v4`) with a 2/3 majority quorum.*

![Fault Tolerance Leader Crash and Automatic Failover](screenshots/3.png)

---

### 6.4 Node Crash Recovery & State Synchronization
*Node 1 restarts, reloads its persisted state (`v3`) from disk, detects a higher cluster version on Node 2, and automatically synchronizes missed updates to reach `v4`.*

![Node Crash Recovery and State Synchronization](screenshots/4.png)