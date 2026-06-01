from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

from config import producer_config
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Counters — use a simple global counter for order IDs so that every run
# gets unique, monotonically increasing IDs.  In production you would use
# a distributed sequence (e.g. Postgres SEQUENCE or Snowflake ID).
# ---------------------------------------------------------------------------
import time
_ORDER_COUNTER: int = int(time.time())


def _next_order_id() -> int:
    global _ORDER_COUNTER
    _ORDER_COUNTER += 1
    return _ORDER_COUNTER


def generate_order() -> Dict[str, Any]:
    """
    Generate a single random food delivery order record.

    Returns:
        A dictionary matching the canonical order schema::

            {
                "order_id":        int,
                "customer_id":     int,
                "restaurant_id":   int,
                "city":            str,
                "item_name":       str,
                "quantity":        int,
                "amount":          float,
                "payment_mode":    str,
                "delivery_status": str,
                "order_time":      str   # ISO-8601 UTC
            }
    """
    cfg = producer_config

    quantity: int = random.randint(1, 5)
    unit_price: float = round(
        random.uniform(cfg.price_range["min"], cfg.price_range["max"]), 2
    )
    amount: float = round(quantity * unit_price, 2)

    order: Dict[str, Any] = {
        "order_id": _next_order_id(),
        "customer_id": random.randint(500, 9999),
        "restaurant_id": random.randint(1, 200),
        "city": random.choice(cfg.cities),
        "item_name": random.choice(cfg.food_items),
        "quantity": quantity,
        "amount": amount,
        "payment_mode": random.choice(cfg.payment_modes),
        "delivery_status": random.choice(cfg.delivery_statuses),
        "order_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

    logger.debug(
        "Generated order | id=%s | city=%s | item=%s | amount=%.2f",
        order["order_id"],
        order["city"],
        order["item_name"],
        order["amount"],
    )
    return order


def generate_invalid_order() -> Dict[str, Any]:
    """
    Intentionally create an invalid order for testing the rejection pipeline.
    Used only in development / testing scenarios.
    """
    bad_order = generate_order()
    corruption_type = random.choice(["negative_amount", "zero_quantity", "missing_city"])

    if corruption_type == "negative_amount":
        bad_order["amount"] = -abs(bad_order["amount"])
    elif corruption_type == "zero_quantity":
        bad_order["quantity"] = 0
    else:  # missing_city
        bad_order["city"] = None  # type: ignore[assignment]

    logger.debug("Generated invalid order (type=%s): %s", corruption_type, bad_order)
    return bad_order
