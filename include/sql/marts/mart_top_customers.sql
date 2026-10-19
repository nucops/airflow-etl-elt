-- Data Mart: mart_top_customers
-- Answers Business Question 5: Top 10 customers by spend, with loyalty tier (no raw email or phone shown).
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.full_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_spend
FROM stg_customers c
JOIN stg_orders o ON c.customer_id = o.customer_id
WHERE o.dq_status = 'PASSED'
  AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10;
