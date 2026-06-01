-- ─── Extensions ───────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- =============================================================================
-- TABLE: orders
-- Raw, validated order records inserted by the Kafka consumer / Spark job.
-- =============================================================================
CREATE TABLE IF NOT EXISTS orders (
    id               BIGSERIAL       PRIMARY KEY,
    order_id         INTEGER         NOT NULL UNIQUE,
    customer_id      INTEGER         NOT NULL,
    restaurant_id    INTEGER         NOT NULL,
    city             VARCHAR(100)    NOT NULL,
    item_name        VARCHAR(200)    NOT NULL,
    quantity         INTEGER         NOT NULL CHECK (quantity > 0),
    amount           NUMERIC(12, 2)  NOT NULL CHECK (amount > 0),
    payment_mode     VARCHAR(50)     NOT NULL,
    delivery_status  VARCHAR(50)     NOT NULL,
    order_time       TIMESTAMP       NOT NULL,
    ingested_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_orders_city            ON orders (city);
CREATE INDEX IF NOT EXISTS idx_orders_item_name       ON orders (item_name);
CREATE INDEX IF NOT EXISTS idx_orders_payment_mode    ON orders (payment_mode);
CREATE INDEX IF NOT EXISTS idx_orders_delivery_status ON orders (delivery_status);
CREATE INDEX IF NOT EXISTS idx_orders_order_time      ON orders (order_time DESC);
CREATE INDEX IF NOT EXISTS idx_orders_ingested_at     ON orders (ingested_at DESC);

COMMENT ON TABLE orders IS
    'Raw validated food delivery orders ingested from Kafka.';

-- =============================================================================
-- TABLE: city_sales_summary
-- Continuously updated aggregations of order revenue by city.
-- =============================================================================
CREATE TABLE IF NOT EXISTS city_sales_summary (
    id            BIGSERIAL       PRIMARY KEY,
    city          VARCHAR(100)    NOT NULL UNIQUE,
    total_orders  BIGINT          NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    last_updated  TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_city_sales_total_revenue
    ON city_sales_summary (total_revenue DESC);

COMMENT ON TABLE city_sales_summary IS
    'Aggregated total orders and revenue grouped by city.';

-- =============================================================================
-- TABLE: food_sales_summary
-- Continuously updated aggregations of revenue by food item.
-- =============================================================================
CREATE TABLE IF NOT EXISTS food_sales_summary (
    id            BIGSERIAL       PRIMARY KEY,
    item_name     VARCHAR(200)    NOT NULL UNIQUE,
    total_orders  BIGINT          NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    last_updated  TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_food_sales_total_revenue
    ON food_sales_summary (total_revenue DESC);

COMMENT ON TABLE food_sales_summary IS
    'Aggregated total orders and revenue grouped by food item.';

-- =============================================================================
-- TABLE: payment_summary
-- Orders and revenue broken down by payment mode.
-- =============================================================================
CREATE TABLE IF NOT EXISTS payment_summary (
    id            BIGSERIAL       PRIMARY KEY,
    payment_mode  VARCHAR(50)     NOT NULL UNIQUE,
    total_orders  BIGINT          NOT NULL DEFAULT 0,
    total_revenue NUMERIC(14, 2)  NOT NULL DEFAULT 0.00,
    last_updated  TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE payment_summary IS
    'Aggregated orders and revenue grouped by payment mode.';

-- =============================================================================
-- TABLE: delivery_summary
-- Orders counted by delivery status.
-- =============================================================================
CREATE TABLE IF NOT EXISTS delivery_summary (
    id               BIGSERIAL   PRIMARY KEY,
    delivery_status  VARCHAR(50) NOT NULL UNIQUE,
    total_orders     BIGINT      NOT NULL DEFAULT 0,
    last_updated     TIMESTAMP   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE delivery_summary IS
    'Order counts grouped by delivery status.';

CREATE TABLE IF NOT EXISTS hourly_volume_summary (
    order_date DATE,
    hour_bucket INTEGER,
    total_orders INTEGER,
    last_updated TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (order_date, hour_bucket)
);
-- =============================================================================
-- VIEWS — Convenient analytical views for Power BI
-- =============================================================================

-- Overall KPIs
CREATE OR REPLACE VIEW vw_overall_kpis AS
SELECT
    COUNT(*)                                      AS total_orders,
    ROUND(SUM(amount), 2)                         AS total_revenue,
    ROUND(AVG(amount), 2)                         AS avg_order_value,
    MIN(order_time)                               AS earliest_order,
    MAX(order_time)                               AS latest_order
FROM orders;

-- Daily Revenue Trend
CREATE OR REPLACE VIEW vw_daily_revenue AS
SELECT
    DATE_TRUNC('day', order_time)::DATE       AS order_date,
    COUNT(*)                                  AS total_orders,
    ROUND(SUM(amount), 2)                     AS total_revenue,
    ROUND(AVG(amount), 2)                     AS avg_order_value
FROM orders
GROUP BY DATE_TRUNC('day', order_time)::DATE
ORDER BY order_date DESC;

-- Hourly Revenue Trend
CREATE OR REPLACE VIEW vw_hourly_revenue AS
SELECT
    DATE_TRUNC('hour', order_time)            AS order_hour,
    COUNT(*)                                  AS total_orders,
    ROUND(SUM(amount), 2)                     AS total_revenue
FROM orders
GROUP BY DATE_TRUNC('hour', order_time)
ORDER BY order_hour DESC;

-- Top 10 Revenue Cities
CREATE OR REPLACE VIEW vw_top_cities AS
SELECT
    city,
    COUNT(*)                  AS total_orders,
    ROUND(SUM(amount), 2)     AS total_revenue,
    ROUND(AVG(amount), 2)     AS avg_order_value
FROM orders
GROUP BY city
ORDER BY total_revenue DESC
LIMIT 10;

-- Top 10 Food Items by Revenue
CREATE OR REPLACE VIEW vw_top_food_items AS
SELECT
    item_name,
    COUNT(*)                  AS total_orders,
    ROUND(SUM(amount), 2)     AS total_revenue,
    ROUND(AVG(amount), 2)     AS avg_order_value
FROM orders
GROUP BY item_name
ORDER BY total_revenue DESC
LIMIT 10;

-- Payment mode distribution
CREATE OR REPLACE VIEW vw_payment_distribution AS
SELECT
    payment_mode,
    COUNT(*)                                          AS total_orders,
    ROUND(SUM(amount), 2)                             AS total_revenue,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS order_pct
FROM orders
GROUP BY payment_mode
ORDER BY total_revenue DESC;

-- Delivery status breakdown
CREATE OR REPLACE VIEW vw_delivery_status AS
SELECT
    delivery_status,
    COUNT(*)                                          AS total_orders,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS order_pct
FROM orders
GROUP BY delivery_status
ORDER BY total_orders DESC;
DO $$
BEGIN
    RAISE NOTICE 'food_delivery_db schema created successfully at %', NOW();
END
$$;
