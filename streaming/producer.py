"""Order event producer for Kafka streaming pipeline."""

import argparse
import json
import os
import random
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load .env
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce simulated order events to Kafka")
    parser.add_argument("--count", type=int, default=300, help="Number of events to generate")
    parser.add_argument("--rate", type=float, default=10.0, help="Target events sent per second")
    parser.add_argument("--dup-every", type=int, default=0, help="Resend a previous event every K events")
    parser.add_argument("--bad-every", type=int, default=0, help="Send an event with amount <= 0 every K events")
    parser.add_argument("--topic", type=str, default="orders", help="Target Kafka topic")
    return parser.parse_args()


def generate_event(is_bad: bool = False) -> dict:
    amount = -round(random.uniform(5.0, 100.0), 2) if is_bad else round(random.uniform(10.0, 500.0), 2)
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"live_ord_{uuid.uuid4().hex[:12]}",
        "product_id": f"live_prod_{random.randint(1, 100)}",
        "amount": amount,
        "event_ts": datetime.now(UTC).isoformat(),
    }


def main() -> None:
    args = parse_args()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = Producer({
        "bootstrap.servers": bootstrap_servers,
        "client.id": "orders-producer",
        "linger.ms": 5,
    })

    print(
        f"Starting producer: count={args.count}, rate={args.rate}/s, "
        f"dup_every={args.dup_every}, bad_every={args.bad_every}",
        flush=True,
    )

    sent_events: list[dict] = []
    distinct_valid_event_ids: set[str] = set()
    dup_count = 0
    bad_count = 0

    sleep_interval = 1.0 / max(args.rate, 0.1)

    for i in range(args.count):
        # Determine if this iteration sends a duplicate or bad event
        is_dup = args.dup_every > 0 and ((i + 1) % args.dup_every == 0) and len(sent_events) > 0
        is_bad = args.bad_every > 0 and ((i + 1) % args.bad_every == 0)

        if is_dup:
            event = random.choice(sent_events)
            dup_count += 1
        else:
            event = generate_event(is_bad=is_bad)
            if is_bad:
                bad_count += 1
            else:
                distinct_valid_event_ids.add(event["event_id"])
            sent_events.append(event)

        payload = json.dumps(event).encode("utf-8")
        producer.produce(
            topic=args.topic,
            key=event["order_id"].encode("utf-8"),
            value=payload,
        )
        producer.poll(0)
        time.sleep(sleep_interval)

    producer.flush(timeout=10.0)

    # Save manifest
    logs_dir = BASE_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = logs_dir / "producer_manifest.json"

    manifest_data = {
        "total_sent": args.count,
        "distinct_valid_count": len(distinct_valid_event_ids),
        "duplicates_sent": dup_count,
        "bad_events_sent": bad_count,
        "valid_event_ids": list(distinct_valid_event_ids),
        "timestamp": datetime.now(UTC).isoformat(),
    }

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

    print(
        f"Producer completed: total_sent={args.count}, distinct_valid={len(distinct_valid_event_ids)}, "
        f"duplicates={dup_count}, bad={bad_count}. Manifest saved to {manifest_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
