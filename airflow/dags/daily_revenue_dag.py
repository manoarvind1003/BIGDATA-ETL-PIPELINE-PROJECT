from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# ---------------------------------------------------------------------------
# SQL snippets
# ---------------------------------------------------------------------------

REFRESH_CITY_SALES = """
    INSERT INTO city_sales_summary (city, total_orders, total_revenue, last_updated)
    SELECT
        city,
        COUNT(*)              AS total_orders,
        ROUND(SUM(amount), 2) AS total_revenue,
        NOW()                 AS last_updated
    FROM orders
    WHERE order_time >= NOW() - INTERVAL '24 hours'
    GROUP BY city
    ON CONFLICT (city) DO UPDATE SET
        total_orders  = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        last_updated  = EXCLUDED.last_updated;
"""

REFRESH_FOOD_SALES = """
    INSERT INTO food_sales_summary (item_name, total_orders, total_revenue, last_updated)
    SELECT
        item_name,
        COUNT(*)              AS total_orders,
        ROUND(SUM(amount), 2) AS total_revenue,
        NOW()                 AS last_updated
    FROM orders
    WHERE order_time >= NOW() - INTERVAL '24 hours'
    GROUP BY item_name
    ON CONFLICT (item_name) DO UPDATE SET
        total_orders  = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        last_updated  = EXCLUDED.last_updated;
"""

REFRESH_PAYMENT = """
    INSERT INTO payment_summary (payment_mode, total_orders, total_revenue, last_updated)
    SELECT
        payment_mode,
        COUNT(*)              AS total_orders,
        ROUND(SUM(amount), 2) AS total_revenue,
        NOW()                 AS last_updated
    FROM orders
    WHERE order_time >= NOW() - INTERVAL '24 hours'
    GROUP BY payment_mode
    ON CONFLICT (payment_mode) DO UPDATE SET
        total_orders  = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        last_updated  = EXCLUDED.last_updated;
"""

REFRESH_DELIVERY = """
    INSERT INTO delivery_summary (delivery_status, total_orders, last_updated)
    SELECT
        delivery_status,
        COUNT(*)  AS total_orders,
        NOW()     AS last_updated
    FROM orders
    WHERE order_time >= NOW() - INTERVAL '24 hours'
    GROUP BY delivery_status
    ON CONFLICT (delivery_status) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        last_updated = EXCLUDED.last_updated;
"""

COUNT_ORDERS = """
    SELECT COUNT(*) AS daily_orders
    FROM orders
    WHERE order_time >= NOW() - INTERVAL '24 hours';
"""

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="daily_revenue_aggregation",
    default_args=default_args,
    description="Nightly refresh of all summary tables from the orders table.",
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    tags=["food-delivery", "aggregation", "daily"],
    max_active_runs=1,
) as dag:

    check_orders = PostgresOperator(
        task_id="check_daily_order_count",
        postgres_conn_id="food_delivery_postgres",
        sql=COUNT_ORDERS,
    )

    refresh_city = PostgresOperator(
        task_id="refresh_city_sales_summary",
        postgres_conn_id="food_delivery_postgres",
        sql=REFRESH_CITY_SALES,
    )

    refresh_food = PostgresOperator(
        task_id="refresh_food_sales_summary",
        postgres_conn_id="food_delivery_postgres",
        sql=REFRESH_FOOD_SALES,
    )

    refresh_payment = PostgresOperator(
        task_id="refresh_payment_summary",
        postgres_conn_id="food_delivery_postgres",
        sql=REFRESH_PAYMENT,
    )

    refresh_delivery = PostgresOperator(
        task_id="refresh_delivery_summary",
        postgres_conn_id="food_delivery_postgres",
        sql=REFRESH_DELIVERY,
    )

    # Each summary refresh runs in parallel after the order count check
    check_orders >> [refresh_city, refresh_food, refresh_payment, refresh_delivery]
