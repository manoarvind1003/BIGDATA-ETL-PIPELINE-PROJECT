from __future__ import annotations

import logging
from functools import partial
from typing import Any, Callable

from pyspark.sql import DataFrame

from config import postgres_config

logger = logging.getLogger("spark.db_writer")


# ---------------------------------------------------------------------------
# JDBC connection properties (reused by all writers)
# ---------------------------------------------------------------------------

def _get_jdbc_props() -> dict:
    cfg = postgres_config
    return {
        "user": cfg.user,
        "password": cfg.password,
        "driver": "org.postgresql.Driver",
    }


# ---------------------------------------------------------------------------
# Generic batch writer
# ---------------------------------------------------------------------------

def write_df_to_postgres(
    df: DataFrame,
    table: str,
    mode: str = "append",
) -> None:
    """
    Write a Spark DataFrame to a PostgreSQL table via JDBC.

    Args:
        df:    DataFrame to write.
        table: Target table name in PostgreSQL.
        mode:  Spark write mode — append or overwrite.
    """

    try:
        # Avoid df.rdd.isEmpty() on Windows
        if len(df.head(1)) == 0:
            logger.info(
                "Empty batch for table '%s' — skipping write.",
                table
            )
            return

        row_count = df.count()

        logger.info(
            "Writing %d rows to PostgreSQL table '%s'...",
            row_count,
            table
        )

        (
            df.write
            .mode(mode)
            .jdbc(
                url=postgres_config.jdbc_url,
                table=table,
                properties=_get_jdbc_props(),
            )
        )

        logger.info(
            "Successfully wrote %d rows to '%s'.",
            row_count,
            table
        )

    except Exception as e:
        logger.exception(
            "Failed writing batch to table '%s': %s",
            table,
            str(e)
        )
        raise


# ---------------------------------------------------------------------------
# foreachBatch sink factories
# These are used as:
#   stream.writeStream.foreachBatch(orders_sink).start()
# ---------------------------------------------------------------------------

def orders_sink(batch_df: DataFrame, batch_id: int) -> None:
    """Write raw validated orders to the ``orders`` table using UPSERT."""
    logger.info("orders_sink | batch_id=%d", batch_id)
    
    try:
        if len(batch_df.head(1)) == 0:
            return
    except Exception:
        return

    cols = [
        "order_id", "customer_id", "restaurant_id", "city", "item_name",
        "quantity", "amount", "payment_mode", "delivery_status", "order_time"
    ]
    
    records = [row.asDict() for row in batch_df.select(*cols).collect()]
    if not records:
        return
        
    from utils.db_utils import get_connection
    import psycopg2.extras
    
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            upsert_sql = """
                INSERT INTO orders (
                    order_id, customer_id, restaurant_id, city, item_name,
                    quantity, amount, payment_mode, delivery_status, order_time
                ) VALUES %s
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    restaurant_id = EXCLUDED.restaurant_id,
                    city = EXCLUDED.city,
                    item_name = EXCLUDED.item_name,
                    quantity = EXCLUDED.quantity,
                    amount = EXCLUDED.amount,
                    payment_mode = EXCLUDED.payment_mode,
                    delivery_status = EXCLUDED.delivery_status,
                    order_time = EXCLUDED.order_time,
                    ingested_at = NOW();
            """
            
            values = [
                (
                    r["order_id"], r["customer_id"], r["restaurant_id"],
                    r["city"], r["item_name"], r["quantity"], r["amount"],
                    r["payment_mode"], r["delivery_status"], r["order_time"]
                )
                for r in records
            ]
            psycopg2.extras.execute_values(cur, upsert_sql, values, page_size=500)
            conn.commit()
            logger.info("Successfully upserted %d rows to 'orders'", len(records))
    except Exception as e:
        conn.rollback()
        logger.error("Failed to upsert to orders: %s", e)
        raise e
    finally:
        conn.close()


def rejected_sink(batch_df: DataFrame, batch_id: int) -> None:
    """Write rejected records to CSV (not PostgreSQL) as a cheap sidecar."""
    logger.info("rejected_sink | batch_id=%d | rows=%d", batch_id, batch_df.count())
    # Write to CSV; the path is relative to SPARK_HOME or the checkpoint dir.
    from config import csv_config
    output_path = str(csv_config.rejected_dir / "spark_rejected")
    (
        batch_df
        .coalesce(1)
        .write
        .mode("append")
        .option("header", "true")
        .csv(output_path)
    )


def aggregation_sink(
    batch_df: DataFrame,
    batch_id: int,
    table: str,
) -> None:
    """
    Generic aggregation sink — upserts the target summary table with
    the latest micro-batch data using psycopg2 execute_values.
    """
    logger.info("aggregation_sink | table=%s | batch_id=%d", table, batch_id)
    try:
        if len(batch_df.head(1)) == 0:
            return
    except Exception:
        return

    records = [row.asDict() for row in batch_df.collect()]
    if not records:
        return
        
    if table == "city_sales_summary":
        pk = "city"
    elif table == "food_sales_summary":
        pk = "item_name"
    elif table == "payment_summary":
        pk = "payment_mode"
    elif table == "delivery_summary":
        pk = "delivery_status"
    else:
        # Fallback
        write_df_to_postgres(batch_df, table, mode="append")
        return

    from utils.db_utils import get_connection
    import psycopg2.extras
    
    columns = list(records[0].keys())
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            col_names = ", ".join(columns)
            set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in columns if c != pk])
            
            if "last_updated" not in columns:
                if set_clause:
                    set_clause += ", last_updated = NOW()"
                else:
                    set_clause = "last_updated = NOW()"
            
            upsert_sql = f"""
                INSERT INTO {table} ({col_names})
                VALUES %s
                ON CONFLICT ({pk}) DO UPDATE SET
                    {set_clause};
            """
            
            values = [tuple(r[c] for c in columns) for r in records]
            
            psycopg2.extras.execute_values(cur, upsert_sql, values, page_size=500)
            conn.commit()
            logger.info("Successfully upserted %d rows to '%s'", len(records), table)
    except Exception as e:
        conn.rollback()
        logger.error("Failed to upsert to %s: %s", table, e)
        raise e
    finally:
        conn.close()


def make_aggregation_sink(table: str) -> Callable[[DataFrame, int], None]:
    """
    Factory that returns a ``foreachBatch`` callable bound to ``table``.

    Usage::

        stream.writeStream.foreachBatch(make_aggregation_sink("city_sales_summary"))
    """
    return partial(aggregation_sink, table=table)

