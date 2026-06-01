from __future__ import annotations

import json
import signal
import sys
import time
import random
from typing import Optional

import click
from confluent_kafka import Producer, KafkaException
from tenacity import retry, stop_after_attempt, wait_fixed

from config import kafka_config, producer_config
from producer.order_generator import generate_order, generate_invalid_order
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_running: bool = True


def _shutdown_handler(signum: int, frame: object) -> None:
    """Gracefully stop the producer loop on SIGINT / SIGTERM."""
    global _running
    logger.info("Shutdown signal received (signal=%d). Flushing Kafka producer…", signum)
    _running = False


signal.signal(signal.SIGINT, _shutdown_handler)
signal.signal(signal.SIGTERM, _shutdown_handler)


# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------

def _delivery_report(err: Optional[Exception], msg: object) -> None:
    """Called by librdkafka when a message has been delivered (or failed)."""
    if err is not None:
        logger.error("Message delivery FAILED | topic=%s | error=%s", msg.topic(), err)  # type: ignore[union-attr]
    else:
        logger.debug(
            "Message delivered | topic=%s | partition=%d | offset=%d",
            msg.topic(),  # type: ignore[union-attr]
            msg.partition(),  # type: ignore[union-attr]
            msg.offset(),  # type: ignore[union-attr]
        )


# ---------------------------------------------------------------------------
# Producer factory
# ---------------------------------------------------------------------------

@retry(stop=stop_after_attempt(10), wait=wait_fixed(3), reraise=True)
def _create_producer() -> Producer:
    """Create a Confluent Kafka producer with retry on connection failure."""
    logger.info("Connecting to Kafka at %s…", kafka_config.bootstrap_servers)
    producer = Producer(kafka_config.producer_config)
    logger.info("Kafka producer connected successfully.")
    return producer


# ---------------------------------------------------------------------------
# Core publish function
# ---------------------------------------------------------------------------

def publish_order(producer: Producer, order: dict, topic: str) -> None:
    """
    Serialize ``order`` to JSON and publish to ``topic``.

    Args:
        producer: Confluent Kafka Producer instance.
        order:    Order dictionary to publish.
        topic:    Target Kafka topic name.
    """
    key = str(order["order_id"]).encode("utf-8")
    value = json.dumps(order, default=str).encode("utf-8")

    producer.produce(
        topic=topic,
        key=key,
        value=value,
        callback=_delivery_report,
    )
    # Poll to trigger delivery callbacks without blocking indefinitely
    producer.poll(0)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

@click.command()
@click.option(
    "--count",
    default=0,
    show_default=True,
    help="Number of orders to produce (0 = run indefinitely).",
)
@click.option(
    "--inject-faults",
    is_flag=True,
    default=False,
    help="Inject ~5%% invalid records to test the rejection pipeline.",
)
def main(count: int, inject_faults: bool) -> None:
    """
    Food Delivery Kafka Producer.

    Generates synthetic order events and publishes them to Kafka.
    """
    global _running

    topic = kafka_config.topic
    cfg = producer_config
    produced = 0

    try:
        producer = _create_producer()
    except KafkaException as exc:
        logger.critical("Cannot connect to Kafka: %s", exc)
        sys.exit(1)

    logger.info(
        "Starting producer | topic=%s | interval=[%ss, %ss] | inject_faults=%s",
        topic,
        cfg.min_interval,
        cfg.max_interval,
        inject_faults,
    )

    try:
        while _running:
            # Optionally inject bad records (5% probability)
            if inject_faults and random.random() < 0.05:
                order = generate_invalid_order()
                logger.warning("Injecting invalid order: id=%s", order.get("order_id"))
            else:
                order = generate_order()

            try:
                publish_order(producer, order, topic)
                produced += 1
                logger.info(
                    "Published order #%d | id=%s | city=%s | item=%s | amount=%.2f",
                    produced,
                    order["order_id"],
                    order["city"],
                    order["item_name"],
                    order["amount"],
                )
            except KafkaException as exc:
                logger.error("Failed to publish order id=%s: %s", order.get("order_id"), exc)

            # Stop if fixed count reached
            if count > 0 and produced >= count:
                logger.info("Reached target count of %d orders. Stopping.", count)
                break

            interval = random.uniform(cfg.min_interval, cfg.max_interval)
            time.sleep(interval)

    finally:
        logger.info("Flushing remaining messages (timeout=15s)…")
        producer.flush(timeout=15)
        logger.info("Producer stopped. Total messages produced: %d", produced)


if __name__ == "__main__":
    main()
