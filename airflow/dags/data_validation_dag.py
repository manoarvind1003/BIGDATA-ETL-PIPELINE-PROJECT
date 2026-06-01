from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2024, 1, 1),
}

# ─── SQL Checks ──────────────────────────────────────────────────────────────

CHECK_NEGATIVE_AMOUNTS = """
    SELECT COUNT(*) AS bad_rows
    FROM orders
    WHERE amount <= 0
    AND ingested_at >= NOW() - INTERVAL '1 hour';
"""

CHECK_ZERO_QUANTITY = """
    SELECT COUNT(*) AS bad_rows
    FROM orders
    WHERE quantity <= 0
    AND ingested_at >= NOW() - INTERVAL '1 hour';
"""

CHECK_NULL_CITY = """
    SELECT COUNT(*) AS bad_rows
    FROM orders
    WHERE city IS NULL OR TRIM(city) = ''
    AND ingested_at >= NOW() - INTERVAL '1 hour';
"""

CHECK_RECENT_INGEST = """
    SELECT EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at))) / 60 AS minutes_since_last
    FROM orders;
"""


def _check_stale_data(**context):
    """Fail if no orders have been ingested in the past 10 minutes."""
    result = context["task_instance"].xcom_pull(task_ids="check_recent_ingest")
    minutes = float(result[0][0]) if result else 999
    if minutes > 10:
        raise ValueError(
            f"No orders ingested in {minutes:.1f} minutes — possible producer failure!"
        )


with DAG(
    dag_id="data_validation",
    default_args=default_args,
    description="Hourly data quality checks on the food_delivery_db orders table.",
    schedule_interval="0 * * * *",  # Every hour
    catchup=False,
    tags=["food-delivery", "data-quality", "monitoring"],
    max_active_runs=1,
) as dag:

    start = DummyOperator(task_id="start")

    check_amounts = PostgresOperator(
        task_id="check_negative_amounts",
        postgres_conn_id="food_delivery_postgres",
        sql=CHECK_NEGATIVE_AMOUNTS,
    )

    check_quantity = PostgresOperator(
        task_id="check_zero_quantity",
        postgres_conn_id="food_delivery_postgres",
        sql=CHECK_ZERO_QUANTITY,
    )

    check_null_city = PostgresOperator(
        task_id="check_null_city",
        postgres_conn_id="food_delivery_postgres",
        sql=CHECK_NULL_CITY,
    )

    check_ingest = PostgresOperator(
        task_id="check_recent_ingest",
        postgres_conn_id="food_delivery_postgres",
        sql=CHECK_RECENT_INGEST,
    )

    validate_staleness = PythonOperator(
        task_id="validate_staleness",
        python_callable=_check_stale_data,
        provide_context=True,
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> [check_amounts, check_quantity, check_null_city, check_ingest]
        >> validate_staleness
        >> end
    )
