"""Kafka order events consumer with manual commits and idempotent PostgreSQL storage."""

import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.etl.load import get_engine

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s]: %(message)s",
)
logger = logging.getLogger("kafka_consumer")


def is_valid_event(event: dict[str, Any]) -> tuple[bool, str]:
    """Validate event schema and business constraints."""
    required = ["event_id", "order_id", "product_id", "amount", "event_ts"]
    for field in required:
        if field not in event or event[field] is None:
            return False, f"missing field '{field}'"

    try:
        amount = float(event["amount"])
        if amount <= 0:
            return False, f"amount {amount} <= 0"
    except (ValueError, TypeError):
        return False, f"unparseable amount '{event.get('amount')}'"

    return True, "valid"


def save_event_to_db(engine: Any, event: dict) -> bool:
    """Save order event to analytics.live_orders using ON CONFLICT DO NOTHING."""
    insert_sql = text(
        """
        INSERT INTO analytics.live_orders (event_id, order_id, product_id, amount, event_ts)
        VALUES (:event_id, :order_id, :product_id, :amount, :event_ts)
        ON CONFLICT (event_id) DO NOTHING;
        """
    )
    with engine.begin() as conn:
        conn.execute(
            insert_sql,
            {
                "event_id": event["event_id"],
                "order_id": event["order_id"],
                "product_id": event["product_id"],
                "amount": event["amount"],
                "event_ts": event["event_ts"],
            },
        )
    return True


def run_consumer(topic: str = "orders", max_messages: int | None = None) -> int:
    """Run consumer loop, committing offsets only after successful DB writes."""
    group_id = os.getenv("KAFKA_GROUP", "live-orders-writer")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
    })

    consumer.subscribe([topic])
    logger.info(f"Consumer started for topic '{topic}' in group '{group_id}'")

    engine = get_engine()
    processed_count = 0
    running = True

    def handle_signal(_signum: int, _frame: Any) -> None:
        nonlocal running
        logger.info("Signal received, stopping consumer...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as err:
                logger.warning(f"Malformed JSON payload, committing and skipping: {err}")
                consumer.commit(msg)
                continue

            # Basic validation
            if not isinstance(data, dict):
                logger.warning("Payload is not a JSON object, committing and skipping")
                consumer.commit(msg)
                continue

            valid, reason = is_valid_event(data)
            if not valid:
                logger.warning(f"Invalid event ({reason}), committing offset and skipping: {data}")
                consumer.commit(msg)
                continue

            # Save to PostgreSQL and commit offset only on DB write success
            try:
                save_event_to_db(engine, data)
                consumer.commit(msg)
                processed_count += 1
            except SQLAlchemyError as db_err:
                logger.error(f"Failed to save event to database: {db_err}")
                continue

            if max_messages and processed_count >= max_messages:
                break
    finally:
        logger.info(f"Closing consumer. Total processed: {processed_count}")
        consumer.close()

    return processed_count


if __name__ == "__main__":
    max_msgs = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run_consumer(max_messages=max_msgs)
