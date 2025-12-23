import os
import time
import redis

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_KEY = os.getenv("STREAM_KEY", "creditcard-transactions")
GROUP_NAME = os.getenv("GROUP_NAME", "fraud-detection-group")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "consumer-1")

def main():
    print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Create consumer group if not exists
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        print(f"Created consumer group '{GROUP_NAME}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{GROUP_NAME}' already exists.")
        else:
            print(f"Error creating group: {e}")
            return

    print(f"Consuming from stream '{STREAM_KEY}' as '{CONSUMER_NAME}' in group '{GROUP_NAME}'...")
    
    while True:
        # Read new messages
        # BLOCK=1000 means wait up to 1 second for new messages
        # COUNT=10 reads up to 10 messages
        entries = r.xreadgroup(GROUP_NAME, CONSUMER_NAME, {STREAM_KEY: ">"}, count=10, block=1000)

        if entries:
            for stream, messages in entries:
                for message_id, message_data in messages:
                    print(f"Processed message {message_id}: {message_data}")
                    # Acknowledge the message (simulating successful processing)
                    r.xack(STREAM_KEY, GROUP_NAME, message_id)
        else:
            # No new messages, tiny sleep to avoid tight loop just in case
            time.sleep(0.1)

if __name__ == "__main__":
    main()
