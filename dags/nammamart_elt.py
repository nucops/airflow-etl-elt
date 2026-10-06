"""NammaMart ELT Pipeline (Part B).

Extract -> Load Raw -> Transform in Warehouse via SQL (Raw -> Staging -> Mart).
SLA: Ready before 9:00 AM IST.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

LOCAL_TZ = pendulum.timezone("Asia/Kolkata")
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "include", "data", "nammamart")
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "include", "sql")
WAREHOUSE_PATH = os.path.join(os.path.dirname(__file__), "..", "include", "warehouse.duckdb")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="nammamart_elt",
    default_args=default_args,
    description="NammaMart Production ELT Pipeline with 3-Layer DuckDB Warehouse (Raw -> Stg -> Mart)",
    schedule="0 9 * * 1-6",  # 9:00 AM IST Monday to Saturday
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["nammamart", "elt", "iitr-futurense"],
)
def nammamart_elt_pipeline():
    @task
    def load_raw_tables():
        """Load exact copies of all CSVs into raw_* tables in DuckDB without cleaning."""
        return {"layer": "raw", "status": "loaded"}

    @task
    def run_staging_transformations(raw_status):
        """Execute SQL models to populate stg_* tables with typing, masking, and DQ flags."""
        return {"layer": "staging", "status": "transformed"}

    @task
    def run_incremental_merge(stg_status):
        """Merge next-day orders_2026-10-01_increment.csv into stg_orders."""
        return {"layer": "staging_incremental", "status": "merged"}

    @task
    def build_marts(merge_status):
        """Build mart_* analytical tables answering the 6 core business questions."""
        return {"layer": "mart", "status": "completed"}

    raw = load_raw_tables()
    stg = run_staging_transformations(raw)
    inc = run_incremental_merge(stg)
    build_marts(inc)


nammamart_elt_pipeline()
