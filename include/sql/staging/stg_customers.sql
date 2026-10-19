-- Staging SQL Model: stg_customers
-- Deduplicates customer records, validates formats, and masks PII conforming to DPDP Act 2023.
CREATE OR REPLACE TABLE stg_customers AS
WITH deduplicated AS (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(full_name) AS full_name,
        TRIM(email) AS email_raw,
        -- Mask email: pr***@example.com
        SUBSTRING(TRIM(email), 1, 2) || '***@' || SPLIT_PART(TRIM(email), '@', 2) AS email_masked,
        TRIM(phone) AS phone_raw,
        -- Mask phone: 98******21
        SUBSTRING(REGEXP_REPLACE(TRIM(phone), '[^0-9]', ''), 1, 2) || '******' || SUBSTRING(REGEXP_REPLACE(TRIM(phone), '[^0-9]', ''), -2) AS phone_masked,
        TRIM(city) AS city,
        TRIM(area) AS area,
        TRIM(pincode) AS pincode,
        CAST(signup_date AS DATE) AS signup_date,
        INITCAP(TRIM(loyalty_tier)) AS loyalty_tier,
        CAST(is_active AS BOOLEAN) AS is_active,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY _loaded_at DESC) AS rn
    FROM raw_customers
)
SELECT
    customer_id,
    full_name,
    email_masked,
    phone_masked,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN customer_id IS NULL OR customer_id = '' THEN 'FLAGGED: Missing customer_id'
        WHEN email_raw NOT LIKE '%@%.%' THEN 'FLAGGED: Malformed email'
        WHEN LENGTH(REGEXP_REPLACE(phone_raw, '[^0-9]', '')) != 10 THEN 'FLAGGED: Non-10-digit phone'
        ELSE 'PASSED'
    END AS dq_status
FROM deduplicated
WHERE rn = 1;
