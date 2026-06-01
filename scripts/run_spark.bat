@echo off
set "PYSPARK_PYTHON=python"
set "PYSPARK_DRIVER_PYTHON=python"
:: scripts/run_spark.bat
:: -----------------------------------------------------------------------------
:: Convenience script to run the Spark Streaming job locally on Windows.
:: It automatically downloads the required JDBC and Kafka packages.
:: -----------------------------------------------------------------------------

cd %~dp0\..

echo ============================================================
echo  Starting Spark Structured Streaming Job                    
echo ============================================================

:: Ensure checkpoints dir exists
if not exist checkpoints\ mkdir checkpoints

:: Run via spark-submit with required packages
:: - spark-sql-kafka for Kafka integration
:: - postgresql for JDBC sink
spark-submit ^
  --master "local[*]" ^
  --name "FoodDeliveryStreaming" ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 ^
  --conf "spark.sql.shuffle.partitions=4" ^
  spark/spark_streaming.py
