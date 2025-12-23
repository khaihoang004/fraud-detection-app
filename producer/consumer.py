import redis

r = redis.Redis(host="redis", port=6379, decode_responses=True)

stream = "cc_stream"
group = "cc_group"
consumer = "c1"

# create consumer group once
try:
    r.xgroup_create(stream, group, id="0", mkstream=True)
except redis.ResponseError as e:
    if "BUSYGROUP" in str(e):
        pass

print("Consumer watching stream...")

while True:
    msgs = r.xreadgroup(group, consumer, {stream: ">"}, count=5, block=5000)
    if not msgs:
        continue
    for _s, events in msgs:
        for msg_id, fields in events:
            print(f"CONSUME {msg_id} | Time={fields['Time']} | Amount={fields['Amount']} | Class={fields['Class']}")
            r.xack(stream, group, msg_id)
