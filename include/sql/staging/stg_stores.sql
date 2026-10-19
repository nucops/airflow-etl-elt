-- Staging SQL Model: stg_stores
-- Casts types and validates pincodes and store capacity.
CREATE OR REPLACE TABLE stg_stores AS
SELECT
    TRIM(store_id) AS store_id,
    TRIM(store_name) AS store_name,
    UPPER(TRIM(zone)) AS zone,
    TRIM(area) AS area,
    TRIM(CAST(pincode AS VARCHAR)) AS pincode,
    CAST(open_date AS DATE) AS open_date,
    TRIM(manager_name) AS manager_name,
    CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    _loaded_at,
    _source_file,
    _run_id,
    CASE
        WHEN LENGTH(TRIM(CAST(pincode AS VARCHAR))) != 6 OR NOT REGEXP_MATCHES(TRIM(CAST(pincode AS VARCHAR)), '^[1-9][0-9]{5}$') THEN 'FLAGGED: Invalid PIN code'
        ELSE 'PASSED'
    END AS dq_status
FROM raw_stores;
