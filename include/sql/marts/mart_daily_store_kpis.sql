-- Data Mart: mart_daily_store_kpis
-- Answers Business Question 1: Daily net revenue by zone and by store (delivered orders only, after discount).
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT CASE WHEN o.order_status = 'DELIVERED' THEN o.order_id END) AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS daily_net_revenue,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes END), 1) AS avg_delivery_minutes
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
GROUP BY o.order_date, s.zone, s.store_id, s.store_name
ORDER BY o.order_date DESC, daily_net_revenue DESC;
