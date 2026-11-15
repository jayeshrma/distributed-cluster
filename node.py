import os
import sys
import json
import time
import uuid
import threading
import urllib.request
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

PEERS = {
    1: "http://127.0.0.1:8001",
    2: "http://127.0.0.1:8002",
    3: "http://127.0.0.1:8003",
}

class DistributedKVNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.state_file = f"node_{node_id}_state.json"
        self.lock = threading.Lock()
        self.store = {}               # key -> {"value": val, "version": int}
        self.dedup_table = {}         # client_id -> last_request_id (At-most-once semantics)
        self.commit_version = 0       # Logical clock / log index
        self.leader_id = 1            # Lowest alive ID becomes Leader
        self.peer_status = {pid: False for pid in PEERS}
        self.peer_status[self.node_id] = True
        self.load_state_from_disk()

    def save_state_to_disk(self):
        """Persists replicated state machine and deduplication table to disk."""
        data = {
            "commit_version": self.commit_version,
            "store": self.store,
            "dedup_table": self.dedup_table
        }
        tmp_file = self.state_file + ".tmp"
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_file, self.state_file)

    def load_state_from_disk(self):
        """Reloads persisted state upon reboot after a crash."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.commit_version = data.get("commit_version", 0)
                    self.store = data.get("store", {})
                    self.dedup_table = data.get("dedup_table", {})
                print(f"[Node {self.node_id}] Loaded persisted state from disk (v{self.commit_version})")
            except Exception as e:
                print(f"[Node {self.node_id}] Failed to load disk state: {e}")

    def rpc_call(self, peer_id, path, payload=None, timeout=1.5):
        url = f"{PEERS[peer_id]}{path}"
        try:
            if payload is not None:
                data = json.dumps(payload).encode("utf-8")
                req = urllib.request.Request(
                    url,
                    data=data,
                    headers={"Content-Type": "application/json", "Connection": "close"},
                    method="POST"
                )
            else:
                req = urllib.request.Request(url, headers={"Connection": "close"}, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def heartbeat_and_election_loop(self):
        """Monitors peers, elects lowest alive node ID as Leader, and syncs state on recovery."""
        while True:
            alive_nodes = [self.node_id]
            for pid in PEERS:
                if pid == self.node_id:
                    continue
                res = self.rpc_call(pid, "/health", timeout=0.8)
                if res and res.get("status") == "ALIVE":
                    alive_nodes.append(pid)
                    self.peer_status[pid] = True
                    if res.get("commit_version", 0) > self.commit_version:
                        self.sync_from_peer(pid)
                else:
                    self.peer_status[pid] = False

            new_leader = min(alive_nodes)
            with self.lock:
                if new_leader != self.leader_id:
                    print(f"[Node {self.node_id}] ELECTION: Failover triggered! New Leader is Node {new_leader}")
                    self.leader_id = new_leader
            time.sleep(1.0)

    def sync_from_peer(self, peer_id):
        state = self.rpc_call(peer_id, "/internal/sync", timeout=1.5)
        if state:
            with self.lock:
                if state["commit_version"] > self.commit_version:
                    self.store = state["store"]
                    self.dedup_table = state["dedup_table"]
                    self.commit_version = state["commit_version"]
                    self.save_state_to_disk()
                    print(f"[Node {self.node_id}] RECOVERED & SYNCED state to v{self.commit_version} from Node {peer_id}")

    def handle_client_write(self, key, value, op, client_id, req_id):
        with self.lock:
            leader = self.leader_id if self.leader_id != self.node_id else None

        if leader is not None:
            print(f"[Node {self.node_id}] Forwarding {op}({key}) to Leader Node {leader}")
            return self.rpc_call(leader, "/kv", {
                "key": key, "value": value, "op": op,
                "client_id": client_id, "req_id": req_id
            }, timeout=3.0)

        with self.lock:
            # At-Most-Once Linearizable Deduplication Check
            if self.dedup_table.get(str(client_id), -1) >= req_id:
                print(f"[Node {self.node_id}] DEDUP: Ignored duplicate request ({client_id}, {req_id})")
                return {
                    "status": "COMMITTED (DEDUP)",
                    "leader": self.node_id,
                    "version": self.commit_version,
                    "value": self.store.get(key, {}).get("value", "")
                }

            self.commit_version += 1
            ver = self.commit_version
            curr_val = self.store.get(key, {}).get("value", "")
            new_val = value if op == "PUT" else (curr_val + value)

            self.store[key] = {"value": new_val, "version": ver}
            self.dedup_table[str(client_id)] = req_id
            self.save_state_to_disk()

        acks = 1
        rep_payload = {
            "key": key, "value": new_val, "version": ver,
            "client_id": str(client_id), "req_id": req_id
        }
        for pid in PEERS:
            if pid == self.node_id:
                continue
            res = self.rpc_call(pid, "/internal/replicate", rep_payload, timeout=1.5)
            if res and res.get("ack"):
                acks += 1

        quorum_size = (len(PEERS) // 2) + 1
        if acks >= quorum_size:
            print(f"[Node {self.node_id}] QUORUM COMMITTED {op}({key}={new_val}) | v{ver} | ACKs: {acks}/{len(PEERS)}")
            return {"status": "COMMITTED", "leader": self.node_id, "version": ver, "acks": acks, "value": new_val}
        else:
            return {"status": "ERROR_NO_QUORUM", "acks": acks}


class RequestHandler(BaseHTTPRequestHandler):
    node: DistributedKVNode = None

    def _send(self, code, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self._send(200, {})

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {
                "status": "ALIVE",
                "node_id": self.node.node_id,
                "role": "LEADER" if self.node.leader_id == self.node.node_id else "BACKUP",
                "leader_id": self.node.leader_id,
                "commit_version": self.node.commit_version,
                "peers": self.node.peer_status,
                "store": self.node.store
            })
        elif self.path == "/internal/sync":
            with self.node.lock:
                self._send(200, {
                    "commit_version": self.node.commit_version,
                    "store": self.node.store,
                    "dedup_table": self.node.dedup_table
                })
        elif self.path.startswith("/kv?key="):
            key = self.path.split("=", 1)[1]
            entry = self.node.store.get(key)
            self._send(200, {
                "found": entry is not None,
                "key": key,
                "value": entry["value"] if entry else None,
                "version": entry["version"] if entry else 0,
                "served_by": self.node.node_id,
                "role": "LEADER" if self.node.leader_id == self.node.node_id else "BACKUP"
            })
        else:
            self._send(404, {"error": "Not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

        if self.path == "/kv":
            res = self.node.handle_client_write(
                key=body["key"],
                value=body["value"],
                op=body.get("op", "PUT"),
                client_id=body.get("client_id", str(uuid.uuid4())),
                req_id=int(body.get("req_id", 1))
            )
            self._send(200 if res else 503, res or {"status": "ERROR"})

        elif self.path == "/internal/replicate":
            with self.node.lock:
                if body["version"] >= self.node.commit_version:
                    self.node.store[body["key"]] = {"value": body["value"], "version": body["version"]}
                    self.node.dedup_table[str(body["client_id"])] = body["req_id"]
                    self.node.commit_version = body["version"]
                    self.node.save_state_to_disk()
                    print(f"[Node {self.node.node_id}] REPLICATED key='{body['key']}' val='{body['value']}' (v{body['version']})")
            self._send(200, {"ack": True, "node_id": self.node.node_id})

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    if len(sys.argv) < 2 or int(sys.argv[1]) not in PEERS:
        print("Usage: python node.py <1|2|3>")
        sys.exit(1)

    nid = int(sys.argv[1])
    port = 8000 + nid
    kv_node = DistributedKVNode(nid)
    RequestHandler.node = kv_node

    threading.Thread(target=kv_node.heartbeat_and_election_loop, daemon=True).start()
    print(f"=== Node {nid} started at http://127.0.0.1:{port} (Initial Leader: Node {kv_node.leader_id}) ===")
    server = ThreadingHTTPServer(("127.0.0.1", port), RequestHandler)
    server.daemon_threads = True
    server.serve_forever()