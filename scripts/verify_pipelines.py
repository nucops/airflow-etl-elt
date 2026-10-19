"""Verify NammaMart ETL and ELT Pipelines end-to-end using DuckDB.

Calculates exact answers for Business Questions 1-6.
"""

from __future__ import annotations

import csv
import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "include", "data", "nammamart")
SQL_DIR = os.path.join(BASE_DIR, "include", "sql")
DB_PATH = os.path.join(BASE_DIR, "include", "warehouse_shubham.duckdb")

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

con = duckdb.connect(DB_PATH)

print("=== 1. Loading Raw Tables ===")
files = {
    "raw_orders": "orders.csv",
    "raw_payments": "payments.csv",
    "raw_customers": "customers.csv",
    "raw_products": "products.csv",
    "raw_stores": "stores.csv",
}

for tbl, fname in files.items():
    fpath = os.path.join(DATA_DIR, fname).replace("\\", "/")
    con.execute(f"""
        CREATE TABLE {tbl} AS 
        SELECT *, 
               CURRENT_TIMESTAMP AS _loaded_at, 
               '{fname}' AS _source_file, 
               'manual_test_run' AS _run_id 
        FROM read_csv_auto('{fpath}', header=True);
    """)
    cnt = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
    print(f"Loaded {tbl}: {cnt} rows")

print("\n=== 2. Creating Staging Models ===")
staging_files = [
    "stg_stores.sql",
    "stg_products.sql",
    "stg_customers.sql",
    "stg_payments.sql",
    "stg_orders.sql",
]

for sf in staging_files:
    p = os.path.join(SQL_DIR, "staging", sf)
    with open(p, "r", encoding="utf-8") as f:
        sql = f.read()
    con.execute(sql)
    tbl_name = sf.replace(".sql", "")
    cnt = con.execute(f"SELECT count(*) FROM {tbl_name}").fetchone()[0]
    print(f"Created {tbl_name}: {cnt} rows")

print("\n=== 3. Incremental Upsert on stg_orders ===")
inc_path = os.path.join(DATA_DIR, "orders_2026-10-01_increment.csv").replace("\\", "/")
con.execute(f"""
    CREATE TEMPORARY TABLE raw_orders_increment AS 
    SELECT *, 
           CURRENT_TIMESTAMP AS _loaded_at, 
           'orders_2026-10-01_increment.csv' AS _source_file, 
           'inc_test_run' AS _run_id 
    FROM read_csv_auto('{inc_path}', header=True);
""")
# Append to raw_orders
con.execute("INSERT INTO raw_orders SELECT * FROM raw_orders_increment;")
print(f"raw_orders total after increment: {con.execute('SELECT count(*) FROM raw_orders').fetchone()[0]}")

# Execute Upsert MERGE into stg_orders
con.execute(f"""
    MERGE INTO stg_orders AS target
    USING (
        SELECT 
            o.order_id,
            TRY_CAST(o.order_ts AS TIMESTAMP) AS order_ts,
            CAST(STRFTIME(TRY_CAST(o.order_ts AS TIMESTAMP), '%Y-%m-%d') AS DATE) AS order_date,
            o.customer_id,
            o.store_id,
            o.product_id,
            TRY_CAST(o.quantity AS INTEGER) AS quantity,
            TRY_CAST(o.unit_price AS DECIMAL(10,2)) AS unit_price,
            TRY_CAST(o.discount_pct AS DECIMAL(5,2)) AS discount_pct,
            TRY_CAST(o.order_amount AS DECIMAL(10,2)) AS order_amount,
            UPPER(TRIM(o.payment_mode)) AS payment_mode,
            UPPER(TRIM(o.order_status)) AS order_status,
            o.channel,
            TRY_CAST(o.delivery_minutes AS INTEGER) AS delivery_minutes,
            ROUND(TRY_CAST(o.quantity AS INTEGER) * TRY_CAST(o.unit_price AS DECIMAL(10,2)) * (1.0 - (TRY_CAST(o.discount_pct AS DECIMAL(5,2)) / 100.0)), 2) AS expected_amount,
            CASE 
                WHEN TRY_CAST(o.quantity AS INTEGER) <= 0 THEN 'FAILED'
                WHEN UPPER(TRIM(o.order_status)) = 'DELIVERED' AND (TRY_CAST(o.delivery_minutes AS INTEGER) < 8 OR TRY_CAST(o.delivery_minutes AS INTEGER) > 120 OR o.delivery_minutes IS NULL) THEN 'FAILED'
                WHEN ABS(TRY_CAST(o.order_amount AS DECIMAL(10,2)) - ROUND(TRY_CAST(o.quantity AS INTEGER) * TRY_CAST(o.unit_price AS DECIMAL(10,2)) * (1.0 - (TRY_CAST(o.discount_pct AS DECIMAL(5,2)) / 100.0)), 2)) > 0.05 THEN 'FAILED'
                ELSE 'PASSED'
            END AS dq_status,
            CASE 
                WHEN o.order_status = 'DELIVERED' THEN TRY_CAST(o.order_amount AS DECIMAL(10,2))
                ELSE 0.0
            END AS net_revenue,
            CASE 
                WHEN o.order_status = 'DELIVERED' AND TRY_CAST(o.delivery_minutes AS DECIMAL(6,1)) <= 15.0 THEN TRUE
                ELSE FALSE
            END AS within_15_min,
            o._loaded_at,
            o._source_file,
            o._run_id
        FROM raw_orders_increment o
    ) AS src
    ON target.order_id = src.order_id
    WHEN MATCHED THEN
        UPDATE SET 
            order_status = src.order_status,
            delivery_minutes = src.delivery_minutes,
            _loaded_at = src._loaded_at,
            _run_id = src._run_id
    WHEN NOT MATCHED THEN
        INSERT (
            order_id, order_ts, order_date, customer_id, store_id, product_id,
            quantity, unit_price, discount_pct, order_amount,
            payment_mode, order_status, channel, delivery_minutes,
            _loaded_at, _source_file, _run_id,
            expected_amount, dq_status, net_revenue, within_15_min
        ) VALUES (
            src.order_id, src.order_ts, src.order_date, src.customer_id, src.store_id, src.product_id,
            src.quantity, src.unit_price, src.discount_pct, src.order_amount,
            src.payment_mode, src.order_status, src.channel, src.delivery_minutes,
            src._loaded_at, src._source_file, src._run_id,
            src.expected_amount, src.dq_status, src.net_revenue, src.within_15_min
        );
""")
print(f"stg_orders count after MERGE: {con.execute('SELECT count(*) FROM stg_orders').fetchone()[0]}")

print("\n=== 4. Creating Analytical Marts ===")
marts = [
    "mart_daily_store_kpis.sql",
    "mart_category_margin.sql",
    "mart_delivery_sla.sql",
    "mart_payment_reconciliation.sql",
    "mart_top_customers.sql",
]

for mf in marts:
    p = os.path.join(SQL_DIR, "marts", mf)
    with open(p, "r", encoding="utf-8") as f:
        sql = f.read()
    con.execute(sql)
    tbl_name = mf.replace(".sql", "")
    cnt = con.execute(f"SELECT count(*) FROM {tbl_name}").fetchone()[0]
    print(f"Created {tbl_name}: {cnt} rows")

print("\n=== 5. Business Questions Evaluation ===")

print("\n--- Question 1: Daily net revenue by zone and by store (sample 5 rows) ---")
q1 = """
SELECT order_date, zone, store_name, total_orders, delivered_orders, daily_net_revenue, avg_delivery_minutes
FROM mart_daily_store_kpis
ORDER BY order_date DESC, daily_net_revenue DESC
LIMIT 5;
"""
for row in con.execute(q1).fetchall():
    print(row)

print("\n--- Question 2: Revenue and gross margin by product category ---")
q2 = """
SELECT category, delivered_orders, total_units_sold, total_revenue, total_cogs, gross_margin, gross_margin_pct
FROM mart_category_margin
ORDER BY total_revenue DESC;
"""
for row in con.execute(q2).fetchall():
    print(row)

print("\n--- Question 3: Average delivery time per store and % delivered within 15 mins ---")
q3 = """
SELECT store_id, store_name, zone, total_delivered_orders, avg_delivery_minutes, delivered_within_15_min, pct_within_15_min
FROM mart_delivery_sla
ORDER BY total_delivered_orders DESC
LIMIT 5;
"""
for row in con.execute(q3).fetchall():
    print(row)

print("\n--- Question 4: Revenue by payment mode and reconciliation status ---")
q4 = """
SELECT payment_mode, total_orders, reconciled_revenue, unreconciled_transactions, unreconciled_amount
FROM mart_payment_reconciliation
ORDER BY reconciled_revenue DESC;
"""
for row in con.execute(q4).fetchall():
    print(row)

print("\n--- Question 5: Top 10 customers by spend (masked PII) ---")
q5 = """
SELECT customer_id, full_name, email_masked, phone_masked, loyalty_tier, total_orders, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;
"""
for row in con.execute(q5).fetchall():
    print(row)

print("\n--- Question 6: Rejected rows by file and reason ---")
q6 = """
SELECT _source_file, dq_status AS failure_reason, COUNT(*) as rejected_count
FROM stg_orders
WHERE dq_status != 'PASSED'
GROUP BY _source_file, dq_status
ORDER BY rejected_count DESC;
"""
for row in con.execute(q6).fetchall():
    print(row)

con.close()
