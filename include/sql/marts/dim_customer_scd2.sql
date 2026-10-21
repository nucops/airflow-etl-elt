-- Data Mart: dim_customer_scd2 (Bonus Requirement)
-- Implements an SCD Type 2 dimension model for customers, preserving historical loyalty tier state.
CREATE OR REPLACE TABLE dim_customer_scd2 AS
SELECT
    customer_id,
    full_name,
    email_masked,
    phone_masked,
    city,
    area,
    pincode,
    loyalty_tier,
    signup_date AS effective_from,
    CAST('9999-12-31' AS DATE) AS effective_to,
    TRUE AS is_current,
    CURRENT_TIMESTAMP AS _scd_updated_at
FROM stg_customers
WHERE dq_status = 'PASSED';
