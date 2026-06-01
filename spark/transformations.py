from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Order Schema
# ---------------------------------------------------------------------------

ORDER_SCHEMA = StructType([
    StructField("order_id",        IntegerType(), nullable=False),
    StructField("customer_id",     IntegerType(), nullable=True),
    StructField("restaurant_id",   IntegerType(), nullable=True),
    StructField("city",            StringType(),  nullable=True),
    StructField("item_name",       StringType(),  nullable=True),
    StructField("quantity",        IntegerType(), nullable=True),
    StructField("amount",          DoubleType(),  nullable=True),
    StructField("payment_mode",    StringType(),  nullable=True),
    StructField("delivery_status", StringType(),  nullable=True),
    StructField("order_time",      StringType(),  nullable=True),
])


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_kafka_stream(raw_df: DataFrame) -> DataFrame:
    """
    Deserialize the raw Kafka message value bytes into a structured DataFrame.

    Args:
        raw_df: Kafka source DataFrame with ``value`` column (binary).

    Returns:
        DataFrame with all order fields as columns, plus ``kafka_timestamp``.
    """
    parsed = (
        raw_df
        .select(
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), ORDER_SCHEMA).alias("data"),
        )
        .select(
            "kafka_timestamp",
            "data.*",
        )
        .withColumn(
            "order_time",
            F.to_timestamp(F.col("order_time"), "yyyy-MM-dd HH:mm:ss"),
        )
    )
    return parsed


# ---------------------------------------------------------------------------
# Data Quality — Filtering
# ---------------------------------------------------------------------------

def apply_data_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split the stream into valid and rejected records.

    Rejection rules:
        - amount  <= 0  or NULL
        - quantity <= 0  or NULL
        - city     is NULL or empty
        - item_name is NULL or empty

    Returns:
        (valid_df, rejected_df) tuple.
    """
    quality_filter = (
        (F.col("amount").isNotNull())
        & (F.col("amount") > 0)
        & (F.col("quantity").isNotNull())
        & (F.col("quantity") > 0)
        & (F.col("city").isNotNull())
        & (F.trim(F.col("city")) != "")
        & (F.col("item_name").isNotNull())
        & (F.trim(F.col("item_name")) != "")
    )

    valid_df = df.filter(quality_filter)
    rejected_df = (
        df.filter(~quality_filter)
        .withColumn("rejected_at", F.current_timestamp())
    )

    return valid_df, rejected_df


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def calc_city_sales(df: DataFrame) -> DataFrame:
    """
    Aggregate total orders and revenue by city within the micro-batch window.
    """
    return (
        df.groupBy("city")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.current_timestamp().alias("last_updated"),
        )
    )


def calc_food_sales(df: DataFrame) -> DataFrame:
    """
    Aggregate total orders and revenue by food item.
    """
    return (
        df.groupBy("item_name")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.current_timestamp().alias("last_updated"),
        )
    )


def calc_payment_summary(df: DataFrame) -> DataFrame:
    """
    Aggregate total orders and revenue by payment mode.
    """
    return (
        df.groupBy("payment_mode")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("amount"), 2).alias("total_revenue"),
            F.current_timestamp().alias("last_updated"),
        )
    )


def calc_delivery_summary(df: DataFrame) -> DataFrame:
    """
    Count orders by delivery status.
    """
    return (
        df.groupBy("delivery_status")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.current_timestamp().alias("last_updated"),
        )
    )


def calc_overall_kpis(df: DataFrame) -> DataFrame:
    """
    Global KPIs: total orders, total revenue, average order value.
    """
    return df.agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("amount"), 2).alias("total_revenue"),
        F.round(F.avg("amount"), 2).alias("avg_order_value"),
        F.current_timestamp().alias("computed_at"),
    )
