-- Staging SQL Model: stg_orders
-- Casts types, standardizes payment_mode, derives metrics, and flags data quality issues.
CREATE OR REPLACE TABLE stg_orders AS
WITH typed_orders AS (
    SELECT
        TRIM(order_id) AS order_id,
        CAST(order_ts AS TIMESTAMP) AS order_ts,
        CAST(STRFTIME(CAST(order_ts AS TIMESTAMP), '%Y-%m-%d') AS DATE) AS order_date,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        CAST(quantity AS INTEGER) AS quantity,
        CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
        CAST(discount_pct AS DECIMAL(5, 2)) AS discount_pct,
        CAST(order_amount AS DECIMAL(10, 2)) AS order_amount,
        UPPER(TRIM(payment_mode)) AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        CASE
            WHEN delivery_minutes IS NOT NULL AND TRIM(delivery_minutes) != '' THEN CAST(delivery_minutes AS DECIMAL(6, 1))
            ELSE NULL
        END AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id
    FROM raw_orders
),
validated_orders AS (
    SELECT
        o.*,
        ROUND(o.quantity * o.unit_price * (1.0 - o.discount_pct / 100.0), 2) AS expected_amount,
        CASE
            WHEN o.quantity <= 0 THEN 'FLAGGED: Quantity <= 0'
            WHEN o.order_status = 'DELIVERED' AND (o.delivery_minutes IS NULL OR o.delivery_minutes < 8 OR o.delivery_minutes > 120) THEN 'FLAGGED: Delivery minutes out of SLA [8, 120]'
            WHEN ABS(o.order_amount - ROUND(o.quantity * o.unit_price * (1.0 - o.discount_pct / 100.0), 2)) > 0.05 THEN 'FLAGGED: Formula calculation mismatch'
            WHEN o.order_ts > TIMESTAMP '2026-10-02 00:00:00' THEN 'FLAGGED: Future order timestamp'
            WHEN o.customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status = 'PASSED') THEN 'FLAGGED: Orphan customer_id'
            WHEN o.product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status = 'PASSED') THEN 'FLAGGED: Orphan product_id'
            WHEN o.store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status = 'PASSED') THEN 'FLAGGED: Orphan store_id'
            ELSE 'PASSED'
        END AS dq_status,
        CASE
            WHEN o.order_status = 'DELIVERED' THEN o.order_amount
            ELSE 0.0
        END AS net_revenue,
        CASE
            WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15.0 THEN TRUE
            ELSE FALSE
        END AS within_15_min
    FROM typed_orders o
)
SELECT * FROM validated_orders;
