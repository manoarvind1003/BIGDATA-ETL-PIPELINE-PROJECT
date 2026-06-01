from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional

import psycopg2
import psycopg2.extras
from psycopg2 import sql
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import postgres_config
from utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(psycopg2.OperationalError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(10),
    reraise=True,
)
def get_connection() -> psycopg2.extensions.connection:
    """
    Return a new psycopg2 connection with retry logic.
    Retries up to 10 times with exponential back-off on OperationalError.
    """
    cfg = postgres_config
    logger.debug("Connecting to PostgreSQL at %s:%s/%s", cfg.host, cfg.port, cfg.database)
    conn = psycopg2.connect(**cfg.dsn)
    conn.autocommit = False
    logger.debug("PostgreSQL connection established.")
    return conn


@contextmanager
def managed_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager that yields a connection and rolls back on error."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def wait_for_postgres(max_retries: int = 30, delay: float = 2.0) -> None:
    """Block until PostgreSQL is reachable (useful at container startup)."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_connection()
            conn.close()
            logger.info("PostgreSQL is ready.")
            return
        except Exception as exc:
            logger.warning(
                "PostgreSQL not ready (attempt %d/%d): %s", attempt, max_retries, exc
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to PostgreSQL after %d attempts." % max_retries)


# ---------------------------------------------------------------------------
# Batch insert helpers
# ---------------------------------------------------------------------------

def bulk_insert_orders(
    records: List[Dict[str, Any]],
    conn: Optional[psycopg2.extensions.connection] = None,
) -> int:
    """
    Insert a batch of validated order records into the ``orders`` table.

    Uses ON CONFLICT DO NOTHING so re-delivered Kafka messages are idempotent.

    Returns:
        Number of rows actually inserted.
    """
    if not records:
        return 0

    insert_sql = """
        INSERT INTO orders (
            order_id, customer_id, restaurant_id, city, item_name,
            quantity, amount, payment_mode, delivery_status, order_time
        ) VALUES %s
        ON CONFLICT (order_id) DO NOTHING;
    """

    values = [
        (
            r["order_id"],
            r["customer_id"],
            r["restaurant_id"],
            r["city"],
            r["item_name"],
            r["quantity"],
            float(r["amount"]),
            r["payment_mode"],
            r["delivery_status"],
            r["order_time"],
        )
        for r in records
    ]

    close_after = conn is None
    if conn is None:
        conn = get_connection()

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, insert_sql, values, page_size=500)
            inserted = cur.rowcount
        if close_after:
            conn.commit()
        logger.info("Inserted %d order records into PostgreSQL.", inserted)
        return inserted
    except Exception:
        if close_after:
            conn.rollback()
        raise
    finally:
        if close_after:
            conn.close()


def upsert_city_sales(
    records: List[Dict[str, Any]],
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """Upsert aggregated city-level sales data."""
    if not records:
        return

    upsert_sql = """
        INSERT INTO city_sales_summary (city, total_orders, total_revenue, last_updated)
        VALUES %s
        ON CONFLICT (city) DO UPDATE SET
            total_orders  = city_sales_summary.total_orders  + EXCLUDED.total_orders,
            total_revenue = city_sales_summary.total_revenue + EXCLUDED.total_revenue,
            last_updated  = EXCLUDED.last_updated;
    """
    values = [
        (r["city"], r["total_orders"], r["total_revenue"], r["last_updated"])
        for r in records
    ]
    _execute_upsert(upsert_sql, values, conn)


def upsert_food_sales(
    records: List[Dict[str, Any]],
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """Upsert aggregated food-item sales data."""
    if not records:
        return

    upsert_sql = """
        INSERT INTO food_sales_summary (item_name, total_orders, total_revenue, last_updated)
        VALUES %s
        ON CONFLICT (item_name) DO UPDATE SET
            total_orders  = food_sales_summary.total_orders  + EXCLUDED.total_orders,
            total_revenue = food_sales_summary.total_revenue + EXCLUDED.total_revenue,
            last_updated  = EXCLUDED.last_updated;
    """
    values = [
        (r["item_name"], r["total_orders"], r["total_revenue"], r["last_updated"])
        for r in records
    ]
    _execute_upsert(upsert_sql, values, conn)


def upsert_payment_summary(
    records: List[Dict[str, Any]],
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """Upsert aggregated payment-mode data."""
    if not records:
        return

    upsert_sql = """
        INSERT INTO payment_summary (payment_mode, total_orders, total_revenue, last_updated)
        VALUES %s
        ON CONFLICT (payment_mode) DO UPDATE SET
            total_orders  = payment_summary.total_orders  + EXCLUDED.total_orders,
            total_revenue = payment_summary.total_revenue + EXCLUDED.total_revenue,
            last_updated  = EXCLUDED.last_updated;
    """
    values = [
        (r["payment_mode"], r["total_orders"], r["total_revenue"], r["last_updated"])
        for r in records
    ]
    _execute_upsert(upsert_sql, values, conn)


def upsert_delivery_summary(
    records: List[Dict[str, Any]],
    conn: Optional[psycopg2.extensions.connection] = None,
) -> None:
    """Upsert aggregated delivery-status data."""
    if not records:
        return

    upsert_sql = """
        INSERT INTO delivery_summary (delivery_status, total_orders, last_updated)
        VALUES %s
        ON CONFLICT (delivery_status) DO UPDATE SET
            total_orders = delivery_summary.total_orders + EXCLUDED.total_orders,
            last_updated = EXCLUDED.last_updated;
    """
    values = [
        (r["delivery_status"], r["total_orders"], r["last_updated"])
        for r in records
    ]
    _execute_upsert(upsert_sql, values, conn)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _execute_upsert(
    query: str,
    values: list,
    conn: Optional[psycopg2.extensions.connection],
) -> None:
    close_after = conn is None
    if conn is None:
        conn = get_connection()

    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, query, values, page_size=500)
        if close_after:
            conn.commit()
    except Exception:
        if close_after:
            conn.rollback()
        raise
    finally:
        if close_after:
            conn.close()
