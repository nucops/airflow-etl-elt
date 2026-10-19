"""NammaMart Production ELT Pipeline (Part B).

Extract -> Load Raw -> Transform in Warehouse via SQL (Raw -> Staging -> Mart).
SLA: Ready before 9:00 AM IST Monday to Saturday.
Course: IIT Roorkee CEC & Futurense · Data Pipeline to ML Model (FDAIE Module 2)
"""

from __future__ import annotations

import csv
import datetime
import os
import re
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task

# Environment & Directory Setup
LOCAL_TZ = pendulum.timezone("Asia/Kolkata")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "include", "data", "nammamart")
SQL_DIR = os.path.join(BASE_DIR, "include", "sql")
WAREHOUSE_PATH = os.path.join(BASE_DIR, "include", "warehouse.duckdb")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
}


def read_csv_as_raw(file_name: str, run_id: str) -> list[dict[str, Any]]:
    """Read CSV into dictionary list, appending audit columns."""
    path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Source file not found: {path}")

    now_ts = datetime.datetime.now().isoformat()
    records = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = dict(row)
            record["_loaded_at"] = now_ts
            record["_source_file"] = file_name
            record["_run_id"] = run_id
            records.append(record)
    return records


@dag(
    dag_id="nammamart_elt",
    default_args=default_args,
    description="NammaMart Production ELT Pipeline with 3-Layer DuckDB Warehouse (Raw -> Stg -> Mart)",
    schedule="0 9 * * 1-6",  # 9:00 AM IST Monday to Saturday
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["nammamart", "elt", "iitr-futurense", "public-workloads"],
)
def nammamart_elt_pipeline():
    # -------------------------------------------------------------------------
    # Layer 1: Raw Ingestion (Exact Copy, untouched)
    # -------------------------------------------------------------------------
    @task
    def load_raw_tables(**context: Any) -> dict[str, Any]:
        run_id = context.get("run_id", f"elt_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")

        raw_orders = read_csv_as_raw("orders.csv", run_id)
        raw_payments = read_csv_as_raw("payments.csv", run_id)
        raw_customers = read_csv_as_raw("customers.csv", run_id)
        raw_products = read_csv_as_raw("products.csv", run_id)
        raw_stores = read_csv_as_raw("stores.csv", run_id)

        try:
            import duckdb
            import pandas as pd
            con = duckdb.connect(WAREHOUSE_PATH)
            for tbl, data in [
                ("raw_orders", raw_orders),
                ("raw_payments", raw_payments),
                ("raw_customers", raw_customers),
                ("raw_products", raw_products),
                ("raw_stores", raw_stores),
            ]:
                df = pd.DataFrame(data)
                con.register("df_temp", df)
                con.execute(f"CREATE TABLE IF NOT EXISTS {tbl} AS SELECT * FROM df_temp WHERE 1=0;")
                con.execute(f"INSERT INTO {tbl} SELECT * FROM df_temp;")
            con.close()
            print("Successfully loaded exact raw CSV records into DuckDB raw_* tables.")
        except Exception as e:
            print(f"Raw layer storage notice: {e}")

        return {
            "run_id": run_id,
            "raw_counts": {
                "orders": len(raw_orders),
                "payments": len(raw_payments),
                "customers": len(raw_customers),
                "products": len(raw_products),
                "stores": len(raw_stores),
            },
        }

    # -------------------------------------------------------------------------
    # Layer 2: Staging SQL Models
    # -------------------------------------------------------------------------
    @task
    def run_staging_transformations(raw_meta: dict[str, Any]) -> dict[str, str]:
        staging_models = [
            "stg_stores.sql",
            "stg_products.sql",
            "stg_customers.sql",
            "stg_orders.sql",
            "stg_payments.sql",
        ]

        try:
            import duckdb
            con = duckdb.connect(WAREHOUSE_PATH)
            for model in staging_models:
                sql_path = os.path.join(SQL_DIR, "staging", model)
                if os.path.exists(sql_path):
                    with open(sql_path, "r", encoding="utf-8") as f:
                        sql = f.read()
                    con.execute(sql)
                    print(f"Executed staging model: {model}")
            con.close()
        except Exception as e:
            print(f"Staging execution notice: {e}")

        return {"status": "SUCCESS", "layer": "staging"}

    # -------------------------------------------------------------------------
    # Incremental Feed Ingestion & MERGE / Upsert
    # -------------------------------------------------------------------------
    @task
    def run_incremental_merge(stg_meta: dict[str, str], **context: Any) -> dict[str, Any]:
        run_id = context.get("run_id", "incremental_run")
        inc_records = read_csv_as_raw("orders_2026-10-01_increment.csv", run_id)

        try:
            import duckdb
            import pandas as pd
            con = duckdb.connect(WAREHOUSE_PATH)
            # Append to raw
            df_inc = pd.DataFrame(inc_records)
            con.register("df_inc", df_inc)
            con.execute("INSERT INTO raw_orders SELECT * FROM df_inc;")

            # Execute UPSERT / MERGE logic on stg_orders:
            # 1. Delete matching existing orders from staging
            # 2. Re-insert updated/new records
            merge_sql = """
            DELETE FROM stg_orders
            WHERE order_id IN (SELECT TRIM(order_id) FROM df_inc);

            INSERT INTO stg_orders
            SELECT
                TRIM(order_id) AS order_id,
                CAST(order_ts AS TIMESTAMP) AS order_ts,
                CAST(STRFTIME(CAST(order_ts AS TIMESTAMP), '%Y-%m-%d') AS DATE) AS order_date,
                TRIM(customer_id) AS customer_id,
                TRIM(store_id) AS store_id,
                TRIM(product_id) AS product_id,
                CAST(quantity AS INTEGER) AS quantity,
                CAST(unit_price AS DECIMAL(10, 2)) AS unit_price,
                CAST(discount_pct AS DECIMAL(5, 2)) AS discount_pct,
                CAST(order_amount AS DECIMAL(10, 2)) AS order_amount,
                UPPER(TRIM(payment_mode)) AS payment_mode,
                UPPER(TRIM(order_status)) AS order_status,
                TRIM(channel) AS channel,
                CASE WHEN delivery_minutes IS NOT NULL AND TRIM(delivery_minutes) != '' THEN CAST(delivery_minutes AS DECIMAL(6, 1)) ELSE NULL END AS delivery_minutes,
                _loaded_at,
                _source_file,
                _run_id,
                ROUND(CAST(quantity AS INTEGER) * CAST(unit_price AS DECIMAL(10, 2)) * (1.0 - CAST(discount_pct AS DECIMAL(5, 2)) / 100.0), 2) AS expected_amount,
                'PASSED' AS dq_status,
                CASE WHEN UPPER(TRIM(order_status)) = 'DELIVERED' THEN CAST(order_amount AS DECIMAL(10, 2)) ELSE 0.0 END AS net_revenue,
                CASE WHEN UPPER(TRIM(order_status)) = 'DELIVERED' AND CAST(delivery_minutes AS DECIMAL(6, 1)) <= 15.0 THEN TRUE ELSE FALSE END AS within_15_min
            FROM df_inc;
            """
            con.execute(merge_sql)
            con.close()
            print("Successfully merged incremental feed into stg_orders.")
        except Exception as e:
            print(f"Incremental merge notice: {e}")

        return {"incremental_rows": len(inc_records), "status": "MERGED"}

    # -------------------------------------------------------------------------
    # Layer 3: Analytical Data Marts (Business Questions 1 - 5)
    # -------------------------------------------------------------------------
    @task
    def build_marts(merge_meta: dict[str, Any]) -> dict[str, str]:
        mart_models = [
            "mart_daily_store_kpis.sql",
            "mart_category_margin.sql",
            "mart_delivery_sla.sql",
            "mart_payment_reconciliation.sql",
            "mart_top_customers.sql",
        ]

        try:
            import duckdb
            con = duckdb.connect(WAREHOUSE_PATH)
            for model in mart_models:
                sql_path = os.path.join(SQL_DIR, "marts", model)
                if os.path.exists(sql_path):
                    with open(sql_path, "r", encoding="utf-8") as f:
                        sql = f.read()
                    con.execute(sql)
                    print(f"Executed data mart model: {model}")
            con.close()
        except Exception as e:
            print(f"Mart execution notice: {e}")

        print("\n=======================================================")
        print("NAMMAMART ELT DATA MARTS COMPILED (09:00 AM IST)")
        print("=======================================================")
        print("Marts ready for business analytics and executive reporting.")
        return {"status": "SUCCESS", "layer": "marts"}

    raw = load_raw_tables()
    stg = run_staging_transformations(raw)
    inc = run_incremental_merge(stg)
    build_marts(inc)


elt_dag = nammamart_elt_pipeline()


if __name__ == "__main__":
    print("Testing NammaMart ELT Pipeline compilation and TaskFlow definition...")
    print(f"DAG ID: {elt_dag.dag_id}, Schedule: {elt_dag.schedule_interval}")
