import os
import json
import time
import pandas as pd
import redis

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_KEY = os.getenv("STREAM_KEY", "creditcard-transactions")
CSV_PATH = os.getenv("CSV_PATH", "data/creditcard.csv")
SLEEP_SECS = float(os.getenv("SLEEP_SECS", "0.01"))  # small delay between messages

def row_to_message(row):
    # Convert pandas Series -> normal dict
    # Redis Streams stores keys/values as strings/bytes. 
    # We can store the whole JSON as a single field or individual fields.
    # Storing as individual fields for flexibility.
    d = row.to_dict()
    # Ensure all values are strings or convertible
    return {k: str(v) for k, v in d.items()}

def main():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Check connection
    try:
        r.ping()
        print("Connected to Redis!")
    except redis.ConnectionError as e:
        print(f"Failed to connect to Redis: {e}")
        return

    # Check if CSV exists, handle relative path logic if needed
    # The original was /app/data/..., here we default to data/creditcard.csv relative to CWD
    if not os.path.exists(CSV_PATH):
        # Fallback for local run from project root
        potential_path = os.path.join("data", "creditcard.csv")
        if os.path.exists(potential_path):
            csv_path_to_use = potential_path
        else:
            print(f"CSV file not found at {CSV_PATH} or {potential_path}")
            return
    else:
        csv_path_to_use = CSV_PATH

    print(f"Loading CSV from {csv_path_to_use}...")
    df = pd.read_csv(csv_path_to_use)

    print(f"Sending {len(df)} messages to stream '{STREAM_KEY}'")
    for idx, row in df.iterrows():
        msg = row_to_message(row)
        # xadd returns the ID of the added message
        r.xadd(STREAM_KEY, msg)

        if idx % 1000 == 0:
            print(f"Sent {idx} messages...")
        time.sleep(SLEEP_SECS)

    print("Done sending all messages.")

if __name__ == "__main__":
    main()
