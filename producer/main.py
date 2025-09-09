# Trick to import necessary modules from the specified directory
import sys
sys.path.insert(0, "libs")

# Import necessary modules from the specified directory
import redis
import time
import random

# Rest of the code...
r = redis.Redis(host='localhost', port=6379, db=0)

streams = ["main-stream", "other-stream", "group-stream"]

print("starting...", flush=True)

for i in range(0, 50):
    sid = random.choice(streams)

    fields = {"kind": f"Event {i}", "priority": i%10}

    # 5% are malformed messages
    if random.uniform(0, 1) < 0.05:
        fields = {"priority": "You-wont-parse-me"}

    r.xadd(sid, fields)

    time.sleep(0.2)

print("finished...", flush=True)