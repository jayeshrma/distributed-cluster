import urllib.request
import json

NODES = ["http://127.0.0.1:8001", "http://127.0.0.1:8002", "http://127.0.0.1:8003"]

def post_json(url, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Connection": "close"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))

def get_json(url):
    req = urllib.request.Request(url, headers={"Connection": "close"}, method="GET")
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.loads(r.read().decode("utf-8"))

print("\n=== TEST 1: Quorum Replication & Strong Consistency ===")
res = post_json(f"{NODES[0]}/kv", {"key": "course", "value": "MIT-6.5840", "op": "PUT", "client_id": "cli-1", "req_id": 101})
print(f"Leader Write Result: {res}")
for i, url in enumerate(NODES, 1):
    read_res = get_json(f"{url}/kv?key=course")
    print(f"  -> Node {i} Read: value='{read_res['value']}', version={read_res['version']}, role={read_res['role']}")
    assert read_res["value"] == "MIT-6.5840", "Replication mismatch!"

print("\n=== TEST 2: At-Most-Once Linearizable Deduplication ===")
print("Sending APPEND '+Lab1' twice with identical (client_id='cli-1', req_id=102)...")
r1 = post_json(f"{NODES[0]}/kv", {"key": "course", "value": "+Lab1", "op": "APPEND", "client_id": "cli-1", "req_id": 102})
r2 = post_json(f"{NODES[0]}/kv", {"key": "course", "value": "+Lab1", "op": "APPEND", "client_id": "cli-1", "req_id": 102})
print(f"  -> First Attempt : {r1['status']} | Value: {r1['value']}")
print(f"  -> Retry Attempt : {r2['status']} | Value: {r2['value']}")
assert r2["value"] == "MIT-6.5840+Lab1", "Deduplication failed! Value was appended twice."

print("\n=== TEST 3: Follower-to-Leader Write Forwarding ===")
r3 = post_json(f"{NODES[2]}/kv", {"key": "consistency", "value": "Linearizable", "op": "PUT", "client_id": "cli-1", "req_id": 103})
print(f"  -> Write sent to Backup Node 3 committed by Leader Node {r3['leader']}: {r3}")

print("\n[SUCCESS] ALL AUTOMATED CONSISTENCY, DEDUPLICATION & REPLICATION TESTS PASSED!\n")