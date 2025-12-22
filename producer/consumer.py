import os
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "creditcard-transactions")
GROUP_ID = os.getenv("GROUP_ID", "creditcard-consumer")

def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    print(f"Consuming from topic '{TOPIC}'...")
    for msg in consumer:
        print(msg.value.decode("utf-8"))

if __name__ == "__main__":
    main()
