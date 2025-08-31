import hashlib, json

def hash_policy(data: dict) -> str:
    raw = json.dumps(data, sort_keys=True, separators=(",",":")).encode()
    return hashlib.sha256(raw).hexdigest()
