-- Data Mart: mart_delivery_sla
-- Answers Business Question 3: Average delivery time per store, and the % of delivered orders within 15 minutes.
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS total_delivered_orders,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes END), 1) AS avg_delivery_minutes,
    COUNT(CASE WHEN o.within_15_min = TRUE THEN 1 END) AS delivered_within_15_min,
    ROUND((COUNT(CASE WHEN o.within_15_min = TRUE THEN 1 END) * 100.0) / NULLIF(COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END), 0), 2) AS pct_within_15_min
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC, avg_delivery_minutes ASC;
