#!/usr/bin/env bash
# scripts/run_spark.sh
# -----------------------------------------------------------------------------
# Convenience script to run the Spark Streaming job locally.
# It automatically downloads the required JDBC and Kafka packages.
# -----------------------------------------------------------------------------
set -e

# Change to project root directory
cd "$(dirname "$0")/.."

echo "============================================================"
echo " Starting Spark Structured Streaming Job                    "
echo "============================================================"

# Ensure checkpoints dir exists
mkdir -p checkpoints/

# Run via spark-submit with required packages
# - spark-sql-kafka for Kafka integration
# - postgresql for JDBC sink
spark-submit \
  --master "local[*]" \
  --name "FoodDeliveryStreaming" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
  --conf "spark.sql.shuffle.partitions=4" \
  spark/spark_streaming.py
