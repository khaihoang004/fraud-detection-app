import os
import json
import time
import pandas as pd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
CSV_PATH = os.getenv("CSV_PATH", "data/creditcard_data.csv")
SLEEP_SECS = float(os.getenv("SLEEP_SECS", "1"))  # 1 sec delay to simulate real-time

def row_to_message(row):
    d = row.to_dict()
    return json.dumps(d).encode("utf-8")

def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=None  # We encode manually
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    print(f"Loading CSV from {CSV_PATH}...")
    try:
        df = pd.read_csv(CSV_PATH)
    except FileNotFoundError:
        print(f"File not found: {CSV_PATH}")
        return

    # Drop Class column if simulating raw input
    if "Class" in df.columns:
        df = df.drop(columns=["Class"])

    print(f"Sending {len(df)} messages to topic '{TOPIC}'")
    for idx, row in df.iterrows():
        msg = row_to_message(row)
        producer.send(TOPIC, msg)
        print(f"Sent message {idx}: Amount={row['Amount']}")
        time.sleep(SLEEP_SECS)

    producer.flush()
    print("Done sending all messages.")

if __name__ == "__main__":
    main()
