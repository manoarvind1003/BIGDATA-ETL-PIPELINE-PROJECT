from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from dotenv import load_dotenv

# Resolve the project root and load the .env file from there
PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _require(key: str) -> str:
    """Return the value of an env variable or raise if missing."""
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            "Please add it to your .env file."
        )
    return value


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: _optional("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(
        default_factory=lambda: _optional("KAFKA_TOPIC", "food_orders")
    )
    group_id: str = field(
        default_factory=lambda: _optional("KAFKA_GROUP_ID", "food_delivery_consumers")
    )
    auto_offset_reset: str = field(
        default_factory=lambda: _optional("KAFKA_AUTO_OFFSET_RESET", "latest")
    )

    @property
    def producer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 500,
            "enable.idempotence": True,
        }

    @property
    def consumer_config(self) -> dict:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        }


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _optional("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(_optional("POSTGRES_PORT", "5432")))
    database: str = field(
        default_factory=lambda: _optional("POSTGRES_DB", "food_delivery_db")
    )
    user: str = field(
        default_factory=lambda: _optional("POSTGRES_USER", "pipeline_user")
    )
    password: str = field(
        default_factory=lambda: _optional("POSTGRES_PASSWORD", "pipeline_pass_2024")
    )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def dsn(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
        }


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SparkConfig:
    app_name: str = field(
        default_factory=lambda: _optional(
            "SPARK_APP_NAME", "FoodDeliveryStreamingPipeline"
        )
    )
    master: str = field(
        default_factory=lambda: _optional("SPARK_MASTER", "local[*]")
    )
    log_level: str = field(
        default_factory=lambda: _optional("SPARK_LOG_LEVEL", "WARN")
    )
    checkpoint_dir: str = field(
        default_factory=lambda: str(PROJECT_ROOT / "checkpoints")
    )
    kafka_package: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    pg_driver_class: str = "org.postgresql.Driver"


# ---------------------------------------------------------------------------
# Order Producer
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProducerConfig:
    min_interval: float = field(
        default_factory=lambda: float(
            _optional("ORDER_PRODUCER_MIN_INTERVAL", "2")
        )
    )
    max_interval: float = field(
        default_factory=lambda: float(
            _optional("ORDER_PRODUCER_MAX_INTERVAL", "5")
        )
    )

    # Domain data
    cities: List[str] = field(default_factory=lambda: [
        "Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad",
        "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow",
    ])
    food_items: List[str] = field(default_factory=lambda: [
        "Chicken Biryani", "Paneer Butter Masala", "Masala Dosa",
        "Veg Burger", "Chicken Tikka", "Margherita Pizza",
        "Pav Bhaji", "Dal Makhani", "Chole Bhature", "Idli Sambar",
        "Mutton Curry", "Veg Fried Rice", "Egg Fried Rice",
        "Veg Noodles", "Shawarma", "Aloo Paratha",
        "Butter Chicken", "Samosa", "Gulab Jamun", "Rasgulla",
    ])
    payment_modes: List[str] = field(default_factory=lambda: [
        "UPI", "Credit Card", "Debit Card", "Cash on Delivery"
    ])
    delivery_statuses: List[str] = field(default_factory=lambda: [
        "Delivered", "Pending", "Cancelled", "Out for Delivery"
    ])
    price_range: dict = field(default_factory=lambda: {"min": 80, "max": 1200})


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LoggingConfig:
    level: str = field(
        default_factory=lambda: _optional("LOG_LEVEL", "INFO")
    )
    log_dir: Path = field(
        default_factory=lambda: PROJECT_ROOT / _optional("LOG_DIR", "logs")
    )


# ---------------------------------------------------------------------------
# CSV Backup
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CsvConfig:
    output_dir: Path = field(
        default_factory=lambda: PROJECT_ROOT
        / _optional("CSV_OUTPUT_DIR", "output/csv_backup")
    )
    rejected_dir: Path = field(
        default_factory=lambda: PROJECT_ROOT
        / _optional("REJECTED_CSV_DIR", "output/rejected_records")
    )


# ---------------------------------------------------------------------------
# Global singleton instances
# ---------------------------------------------------------------------------

kafka_config = KafkaConfig()
postgres_config = PostgresConfig()
spark_config = SparkConfig()
producer_config = ProducerConfig()
logging_config = LoggingConfig()
csv_config = CsvConfig()
