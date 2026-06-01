from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure project root is on sys.path when running via spark-submit
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Load .env before importing config
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import kafka_config, postgres_config, spark_config, csv_config
from spark.transformations import (
    apply_data_quality,
    calc_city_sales,
    calc_delivery_summary,
    calc_food_sales,
    calc_overall_kpis,
    calc_payment_summary,
    parse_kafka_stream,
)
from spark.db_writer import (
    make_aggregation_sink,
    orders_sink,
    rejected_sink,
)

logger = logging.getLogger("spark.streaming")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ---------------------------------------------------------------------------
# SparkSession factory
# ---------------------------------------------------------------------------

def create_spark_session() -> SparkSession:
    """Build and return a configured SparkSession."""
    cfg = spark_config
    pg_cfg = postgres_config

    spark = (
        SparkSession.builder
        .appName(cfg.app_name)
        .master(cfg.master)
        # ── Kafka
        .config("spark.jars.packages", cfg.kafka_package + ",org.postgresql:postgresql:42.7.3")
        # ── Streaming tuning
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        # ── Shuffle partitions: keep low for local mode
        .config("spark.sql.shuffle.partitions", "4")
        # ── Checkpoint compression
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(cfg.log_level)
    logger.info("SparkSession created | app=%s | master=%s", cfg.app_name, cfg.master)
    return spark


# ---------------------------------------------------------------------------
# Kafka source
# ---------------------------------------------------------------------------

def create_kafka_stream(spark: SparkSession):
    """Return the raw Kafka streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config.bootstrap_servers)
        .option("subscribe", kafka_config.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .load()
    )


# ---------------------------------------------------------------------------
# Streaming queries
# ---------------------------------------------------------------------------

def start_orders_stream(valid_df, checkpoint_base: Path):
    """Write raw validated orders to PostgreSQL."""
    return (
        valid_df.writeStream
        .foreachBatch(orders_sink)
        .option("checkpointLocation", str(checkpoint_base / "orders"))
        .trigger(processingTime="10 seconds")
        .start()
    )


def start_rejected_stream(rejected_df, checkpoint_base: Path):
    """Write rejected records to CSV."""
    return (
        rejected_df.writeStream
        .foreachBatch(rejected_sink)
        .option("checkpointLocation", str(checkpoint_base / "rejected"))
        .trigger(processingTime="10 seconds")
        .start()
    )


def start_aggregation_stream(agg_df, table: str, checkpoint_base: Path):
    """Write an aggregated DataFrame to a PostgreSQL summary table."""
    return (
        agg_df.writeStream
        .foreachBatch(make_aggregation_sink(table))
        .option("checkpointLocation", str(checkpoint_base / table))
        .trigger(processingTime="30 seconds")
        .outputMode("complete")
        .start()
    )


def start_console_kpi_stream(kpi_df):
    """Print KPI metrics to the console for real-time visibility."""
    return (
        kpi_df.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("complete")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    spark = create_spark_session()
    checkpoint_base = Path(spark_config.checkpoint_dir)
    checkpoint_base.mkdir(parents=True, exist_ok=True)

    logger.info("Reading stream from Kafka topic: %s", kafka_config.topic)
    raw_stream = create_kafka_stream(spark)

    # ── Parse JSON ─────────────────────────────────────────────────────────
    parsed_stream = parse_kafka_stream(raw_stream)

    # ── Apply Data Quality ─────────────────────────────────────────────────
    # NOTE: We cache the parsed stream using a watermark so we can fan-out
    # to multiple sinks efficiently.
    watermarked = parsed_stream.withWatermark("order_time", "10 minutes")
    valid_df, rejected_df = apply_data_quality(watermarked)

    # ── Aggregations (stateful, windowed) ──────────────────────────────────
    city_agg    = calc_city_sales(valid_df)
    food_agg    = calc_food_sales(valid_df)
    payment_agg = calc_payment_summary(valid_df)
    delivery_agg= calc_delivery_summary(valid_df)
    kpi_agg     = calc_overall_kpis(valid_df)

    # ── Start all streaming queries ────────────────────────────────────────
    logger.info("Starting streaming queries…")

    queries = [
        start_orders_stream(valid_df, checkpoint_base),
        start_rejected_stream(rejected_df, checkpoint_base),
        start_aggregation_stream(city_agg,     "city_sales_summary",  checkpoint_base),
        start_aggregation_stream(food_agg,     "food_sales_summary",  checkpoint_base),
        start_aggregation_stream(payment_agg,  "payment_summary",     checkpoint_base),
        start_aggregation_stream(delivery_agg, "delivery_summary",    checkpoint_base),
        start_console_kpi_stream(kpi_agg),
    ]

    logger.info("All streaming queries started. Awaiting termination…")

    # Block until any query terminates (or all do)
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received — stopping all queries…")
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception as exc:
                logger.warning("Error stopping query: %s", exc)
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    main()
