import time
import redis

r = redis.Redis(host="redis", port=6379, decode_responses=True)
stream = "cc_stream"
group = "cc_group"
RESET_STREAM = True


def safe(call, default=None):
    try:
        return call()
    except Exception:
        return default

if RESET_STREAM:
    r.delete("cc_stream")
    
while True:
    xlen = safe(lambda: r.xlen(stream), 0)
    groups = safe(lambda: r.xinfo_groups(stream), [])
    g = next((x for x in groups if x.get("name") == group), None) or {}

    lag = g.get("lag", "NA")
    pending = g.get("pending", "NA")
    last_id = g.get("last-delivered-id", "NA")

    print(f"📈 XLEN={xlen} | lag={lag} | pending={pending} | last={last_id}", flush=True)
    time.sleep(2)
