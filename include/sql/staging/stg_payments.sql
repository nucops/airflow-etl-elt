-- Staging SQL Model: stg_payments
-- Casts types, standardizes payment_mode, and validates reconciliation status against orders.
CREATE OR REPLACE TABLE stg_payments AS
WITH typed_payments AS (
    SELECT
        TRIM(payment_id) AS payment_id,
        TRIM(order_id) AS order_id,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        UPPER(TRIM(payment_mode)) AS payment_mode,
        TRY_CAST(amount_paid AS DECIMAL(10, 2)) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        TRIM(gateway_ref) AS gateway_ref,
        _loaded_at,
        _source_file,
        _run_id
    FROM raw_payments
)
SELECT
    p.*,
    CASE
        WHEN p.order_id NOT IN (SELECT order_id FROM raw_orders) THEN 'FLAGGED: Orphan order_id'
        WHEN p.payment_status = 'SUCCESS' AND p.amount_paid <= 0 THEN 'FLAGGED: Invalid success amount'
        ELSE 'PASSED'
    END AS dq_status
FROM typed_payments p;
