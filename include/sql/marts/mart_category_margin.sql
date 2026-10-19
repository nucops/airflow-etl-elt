-- Data Mart: mart_category_margin
-- Answers Business Question 2: Revenue and gross margin by product category (margin = revenue - quantity * cost_price).
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    COUNT(DISTINCT o.order_id) AS delivered_orders,
    SUM(o.quantity) AS total_units_sold,
    ROUND(SUM(o.net_revenue), 2) AS total_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS total_cogs,
    ROUND(SUM(o.net_revenue) - SUM(o.quantity * p.cost_price), 2) AS gross_margin,
    ROUND(((SUM(o.net_revenue) - SUM(o.quantity * p.cost_price)) / NULLIF(SUM(o.net_revenue), 0)) * 100.0, 2) AS gross_margin_pct
FROM stg_orders o
JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY total_revenue DESC;
