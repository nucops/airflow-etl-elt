-- Data Mart: mart_payment_reconciliation
-- Answers Business Question 4: Revenue by payment mode, and total amount that doesn't reconcile with the gateway.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH joined_data AS (
    SELECT
        COALESCE(o.payment_mode, p.payment_mode, 'UNKNOWN') AS payment_mode,
        o.order_id,
        o.order_amount,
        p.payment_id,
        p.amount_paid,
        p.payment_status,
        CASE
            WHEN o.order_id IS NULL THEN 'Orphan payment'
            WHEN p.payment_id IS NULL AND o.order_status = 'DELIVERED' THEN 'Missing gateway payment'
            WHEN p.payment_status = 'SUCCESS' AND ABS(COALESCE(o.order_amount, 0) - COALESCE(p.amount_paid, 0)) > 0.05 THEN 'Amount discrepancy'
            ELSE 'Reconciled'
        END AS recon_status
    FROM stg_orders o
    FULL OUTER JOIN stg_payments p ON o.order_id = p.order_id
)
SELECT
    payment_mode,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(CASE WHEN recon_status = 'Reconciled' THEN order_amount ELSE 0 END), 2) AS reconciled_revenue,
    COUNT(CASE WHEN recon_status != 'Reconciled' THEN 1 END) AS unreconciled_transactions,
    ROUND(SUM(CASE WHEN recon_status != 'Reconciled' THEN COALESCE(order_amount, amount_paid, 0) ELSE 0 END), 2) AS unreconciled_amount
FROM joined_data
GROUP BY payment_mode
ORDER BY reconciled_revenue DESC;
