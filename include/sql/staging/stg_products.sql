-- Staging SQL Model: stg_products
-- Casts pricing, deduplicates products, and validates margins.
CREATE OR REPLACE TABLE stg_products AS
WITH deduplicated AS (
    SELECT
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(category) AS category,
        TRIM(sub_category) AS sub_category,
        TRIM(brand) AS brand,
        CAST(mrp AS DECIMAL(10, 2)) AS mrp,
        CAST(cost_price AS DECIMAL(10, 2)) AS cost_price,
        TRIM(unit) AS unit,
        CAST(is_perishable AS BOOLEAN) AS is_perishable,
        CAST(launch_date AS DATE) AS launch_date,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY TRIM(product_id) ORDER BY _loaded_at DESC) AS rn
    FROM raw_products
)
SELECT
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN cost_price <= 0 THEN 'FLAGGED: Non-positive cost_price'
        WHEN cost_price > mrp THEN 'FLAGGED: Cost price exceeds MRP'
        ELSE 'PASSED'
    END AS dq_status
FROM deduplicated
WHERE rn = 1;
