"""Script to wait for Kafka broker readiness and ensure the orders topic exists."""

import os
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

# Load .env from project root
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def create_orders_topic(
    topic_name: str = "orders",
    num_partitions: int = 3,
    replication_factor: int = 1,
    max_wait_seconds: int = 60,
) -> bool:
    """Wait until the Kafka broker answers and ensure the orders topic exists."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    print(f"Connecting to Kafka broker at {bootstrap_servers}...", flush=True)
    start_time = time.time()
    cluster_metadata = None

    while time.time() - start_time < max_wait_seconds:
        try:
            cluster_metadata = admin.list_topics(timeout=3.0)
            if cluster_metadata and cluster_metadata.brokers:
                print(f"Connected to Kafka broker (found {len(cluster_metadata.brokers)} broker(s)).", flush=True)
                break
        except (KafkaException, OSError) as e:
            print(f"Waiting for Kafka broker ({e})...", flush=True)
        time.sleep(2)

    if not cluster_metadata or not cluster_metadata.brokers:
        print(f"ERROR: Kafka broker at {bootstrap_servers} did not respond within {max_wait_seconds}s.", flush=True)
        return False

    if topic_name in cluster_metadata.topics:
        existing = cluster_metadata.topics[topic_name]
        print(f"Topic '{topic_name}' already exists with {len(existing.partitions)} partition(s).", flush=True)
        return True

    print(f"Creating topic '{topic_name}' ({num_partitions} partitions, rep={replication_factor})...", flush=True)
    new_topic = NewTopic(
        topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    futures = admin.create_topics([new_topic])
    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic '{topic}' created successfully.", flush=True)
            return True
        except KafkaException as e:
            print(f"Failed to create topic '{topic}': {e}", flush=True)
            return False

    return False


def main() -> None:
    success = create_orders_topic()
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
