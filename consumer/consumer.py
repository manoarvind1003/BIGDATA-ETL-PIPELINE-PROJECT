from __future__ import annotations

import csv
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import click
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

from config import kafka_config, csv_config, postgres_config
from utils.db_utils import bulk_insert_orders, wait_for_postgres
from utils.logger import get_logger
from utils.validators import validate_order

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------
_running: bool = True


def _shutdown_handler(signum: int, frame: object) -> None:
    global _running
    logger.info("Received signal %d — shutting down consumer gracefully…", signum)
    _running = False


signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)


# ---------------------------------------------------------------------------
# Rejection sink
# ---------------------------------------------------------------------------

class RejectionWriter:
    """
    Appends invalid records to a daily CSV file in the rejection output dir.
    """

    FIELDNAMES = [
        "order_id", "customer_id", "restaurant_id", "city", "item_name",
        "quantity", "amount", "payment_mode", "delivery_status", "order_time",
        "_validation_errors", "_rejected_at",
    ]

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._writer: Optional[csv.DictWriter] = None
        self._file = None
        self._current_date: Optional[str] = None

    def _get_writer(self) -> csv.DictWriter:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._current_date:
            # Rotate to a new file each day
            if self._file:
                self._file.close()
            filepath = self._output_dir / f"rejected_{today}.csv"
            is_new = not filepath.exists()
            self._file = open(filepath, "a", newline="", encoding="utf-8")
            self._writer = csv.DictWriter(
                self._file, fieldnames=self.FIELDNAMES, extrasaction="ignore"
            )
            if is_new:
                self._writer.writeheader()
            self._current_date = today
        return self._writer  # type: ignore[return-value]

    def write(self, record: Dict[str, Any], errors: List[str]) -> None:
        enriched = dict(record)
        enriched["_validation_errors"] = "; ".join(errors)
        enriched["_rejected_at"] = datetime.now(timezone.utc).isoformat()
        writer = self._get_writer()
        writer.writerow(enriched)
        if self._file:
            self._file.flush()

    def close(self) -> None:
        if self._file:
            self._file.close()


# ---------------------------------------------------------------------------
# Core consumer logic
# ---------------------------------------------------------------------------

def _parse_message(msg: Message) -> Optional[Dict[str, Any]]:
    """Deserialize a Kafka message value from JSON."""
    try:
        raw = msg.value()
        if raw is None:
            return None
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error(
            "Failed to deserialize message at partition=%d offset=%d: %s",
            msg.partition(),
            msg.offset(),
            exc,
        )
        return None


@click.command()
@click.option(
    "--batch-size",
    default=20,
    show_default=True,
    help="Number of messages to accumulate before a bulk PostgreSQL insert.",
)
@click.option(
    "--poll-timeout",
    default=1.0,
    show_default=True,
    help="Kafka poll timeout in seconds.",
)
@click.option(
    "--max-messages",
    default=0,
    show_default=True,
    help="Stop after consuming this many messages (0 = run indefinitely).",
)
@click.option(
    "--skip-db",
    is_flag=True,
    default=False,
    help="Validate and log only — skip PostgreSQL writes (for testing).",
)
def main(
    batch_size: int,
    poll_timeout: float,
    max_messages: int,
    skip_db: bool,
) -> None:
    """
    Food Delivery Kafka Consumer.

    Consumes orders from Kafka, validates them, and persists to PostgreSQL.
    """
    logger.info(
        "Starting consumer | topic=%s | group=%s | batch_size=%d | skip_db=%s",
        kafka_config.topic,
        kafka_config.group_id,
        batch_size,
        skip_db,
    )

    if not skip_db:
        wait_for_postgres()

    rejection_writer = RejectionWriter(csv_config.rejected_dir)

    consumer = Consumer(kafka_config.consumer_config)
    consumer.subscribe([kafka_config.topic])
    logger.info("Subscribed to topic: %s", kafka_config.topic)

    valid_batch: List[Dict[str, Any]] = []
    total_consumed = 0
    total_valid = 0
    total_invalid = 0

    try:
        while _running:
            msg: Optional[Message] = consumer.poll(timeout=poll_timeout)

            if msg is None:
                # No message available — flush any pending batch
                if valid_batch:
                    _flush_batch(valid_batch, skip_db)
                    total_valid += len(valid_batch)
                    valid_batch.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "Reached end of partition | %s [%d] @ %d",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue

            record = _parse_message(msg)
            if record is None:
                continue

            total_consumed += 1
            result = validate_order(record)

            if result.is_valid:
                valid_batch.append(record)
                logger.info(
                    "Valid   | #%d | order_id=%s | city=%s | amount=%.2f",
                    total_consumed,
                    record.get("order_id"),
                    record.get("city"),
                    float(record.get("amount", 0)),
                )
            else:
                total_invalid += 1
                rejection_writer.write(record, result.errors)
                logger.warning(
                    "INVALID | #%d | order_id=%s | errors=%s",
                    total_consumed,
                    record.get("order_id"),
                    result.errors,
                )

            # Flush batch when it reaches the threshold
            if len(valid_batch) >= batch_size:
                _flush_batch(valid_batch, skip_db)
                total_valid += len(valid_batch)
                valid_batch.clear()

            # Stop if fixed count reached
            if max_messages > 0 and total_consumed >= max_messages:
                logger.info("Reached max_messages=%d. Stopping.", max_messages)
                break

    except KafkaException as exc:
        logger.critical("Fatal Kafka exception: %s", exc)
        sys.exit(1)
    finally:
        # Final flush
        if valid_batch and not skip_db:
            _flush_batch(valid_batch, skip_db)
            total_valid += len(valid_batch)

        consumer.close()
        rejection_writer.close()

        logger.info(
            "Consumer stopped | Total: %d | Valid: %d | Invalid: %d",
            total_consumed,
            total_valid,
            total_invalid,
        )


def _flush_batch(batch: List[Dict[str, Any]], skip_db: bool) -> None:
    """Persist a batch of valid records to PostgreSQL."""
    if skip_db:
        logger.info("[SKIP-DB] Would insert %d records.", len(batch))
        return
    try:
        bulk_insert_orders(batch)
        logger.info("Flushed %d records to PostgreSQL.", len(batch))
    except Exception as exc:
        logger.error("Failed to flush batch to PostgreSQL: %s", exc)
        raise


if __name__ == "__main__":
    main()
