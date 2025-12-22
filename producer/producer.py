import os
import json
import time
import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "creditcard-transactions")
CSV_PATH = os.getenv("CSV_PATH", "/app/data/creditcard.csv")
SLEEP_SECS = float(os.getenv("SLEEP_SECS", "0.01"))  # small delay between messages

def row_to_message(row):
    # Convert pandas Series -> normal dict, then to JSON
    d = row.to_dict()
    # Most credit card fraud datasets have 'Class' as label; keep it.
    return json.dumps(d).encode("utf-8")

def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    print(f"Loading CSV from {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)

    print(f"Sending {len(df)} messages to topic '{TOPIC}'")
    for idx, row in df.iterrows():
        msg = row_to_message(row)
        producer.send(TOPIC, msg)

        if idx % 1000 == 0:
            print(f"Sent {idx} messages...")
        time.sleep(SLEEP_SECS)

    producer.flush()
    print("Done sending all messages. Sleeping to keep pod alive...")
    # keep pod alive for debugging
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
