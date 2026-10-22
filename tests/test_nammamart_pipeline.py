"""
tests/test_nammamart_pipeline.py
---------------------------------
Comprehensive automated test suite for NammaMart Quick Commerce data pipelines:
  1. Dataset existence & structure checks.
  2. Data Quality & quarantine rule validation.
  3. Star schema (ETL) and Staging/Marts (ELT) warehouse verification.
  4. 100% September Delivered Net Revenue Parity Audit (INR 1,068,148.90, INR 0.00 diff).
  5. SCD Type 2 customer dimension logic.
  6. Machine learning revenue forecasting model & predictions persistence.
  7. AI failure explainer agent & safe Slack notifier fallback.
"""

import os
import sys
import subprocess
import pytest
import pandas as pd
import duckdb

# Add include and scripts directory to path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
INCLUDE_DIR = os.path.join(REPO_ROOT, "include")
DATA_DIR = os.path.join(INCLUDE_DIR, "data", "nammamart")
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")

for p in [REPO_ROOT, INCLUDE_DIR, SCRIPTS_DIR]:
    if p not in sys.path:
        sys.path.insert(0, p)


# ── Test Group 1: Data Assets ─────────────────────────────────────────────────

def test_nammamart_raw_datasets_exist():
    """Validates that all 5 raw source files and the increment file exist and are non-empty."""
    required_files = [
        "orders.csv",
        "payments.csv",
        "customers.csv",
        "products.csv",
        "stores.csv",
        "orders_2026-10-01_increment.csv",
    ]
    for fname in required_files:
        fpath = os.path.join(DATA_DIR, fname)
        assert os.path.exists(fpath), f"Missing required dataset: {fpath}"
        assert os.path.getsize(fpath) > 0, f"Dataset is empty: {fpath}"


def test_upstream_reference_datasets_exist():
    """Validates that upstream reference datasets exist for full upstream scaffold parity."""
    upstream_data_dir = os.path.join(INCLUDE_DIR, "data")
    for fname in ["sales.csv", "customers.csv", "inventory.csv"]:
        fpath = os.path.join(upstream_data_dir, fname)
        assert os.path.exists(fpath), f"Missing upstream reference file: {fpath}"


# ── Test Group 2: Pipeline Execution & Revenue Parity Audit ───────────────────

def test_pipeline_verification_and_revenue_parity():
    """
    Executes the end-to-end pipeline verification script (scripts/verify_pipelines.py)
    and verifies that:
      - All staging, marts, and star schema tables load without errors.
      - Both ETL and ELT delivered revenue match exactly at INR 1,068,148.90 (INR 0.00 diff).
      - DuckDB warehouse database is synchronized.
    """
    script_path = os.path.join(SCRIPTS_DIR, "verify_pipelines.py")
    result = subprocess.run(
        [sys.executable, script_path],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"verify_pipelines.py failed:\n{result.stderr}\n{result.stdout}"
    assert "Revenue Parity Difference: INR 0.00 (Exact 100% Match!)" in result.stdout
    assert "ETL September Delivered Net Revenue: INR 1,068,148.90" in result.stdout
    assert "ELT September Delivered Net Revenue: INR 1,068,148.90" in result.stdout

    # Verify warehouse file existence and contents
    db_path = os.path.join(INCLUDE_DIR, "warehouse_shubham.duckdb")
    assert os.path.exists(db_path), "warehouse_shubham.duckdb must exist"
    con = duckdb.connect(db_path, read_only=True)
    try:
        marts = con.execute("SHOW TABLES").fetchall()
        table_names = [m[0] for m in marts]
        expected_tables = [
            "raw_orders", "stg_orders", "mart_daily_store_kpis",
            "mart_category_margin", "mart_delivery_sla", "dim_customer_scd2",
            "etl_fact_orders", "dq_results", "pipeline_runs"
        ]
        for tbl in expected_tables:
            assert tbl in table_names, f"Expected table {tbl} in warehouse"
    finally:
        con.close()


# ── Test Group 3: Downstream Machine Learning Model ───────────────────────────

def test_ml_revenue_forecasting_model():
    """
    Validates that Scikit-Learn LinearRegression model trains successfully,
    computes feature weights, and writes predictions into mart_revenue_forecast.
    """
    from ml_forecast import run_ml_forecast

    test_db = os.path.join(INCLUDE_DIR, "warehouse_shubham.duckdb")
    if not os.path.exists(test_db):
        test_db = os.path.join(INCLUDE_DIR, "warehouse.duckdb")

    assert os.path.exists(test_db), f"Warehouse database not found at {test_db}"

    results = run_ml_forecast(test_db)
    assert results is not None, "ML forecast returned None"
    assert results["status"] == "success"
    assert results["mae"] > 0, "MAE must be greater than 0"
    assert results["predicted_7d_total"] > 100000, "7-day revenue forecast should be realistic (> 100,000 INR)"

    # Verify DuckDB persistence
    con = duckdb.connect(test_db, read_only=True)
    try:
        forecast_df = con.execute("SELECT * FROM mart_revenue_forecast").fetchdf()
        assert len(forecast_df) > 0, "mart_revenue_forecast table must contain projected rows"
        predictions_log = con.execute("SELECT * FROM ml_predictions").fetchdf()
        assert len(predictions_log) > 0, "ml_predictions audit table must log execution metrics"
    finally:
        con.close()


# ── Test Group 4: AI Failure Explainer & Notifications ────────────────────────

def test_ai_explainer_offline_resilience():
    """
    Verifies that the LangGraph AI failure explainer generates valid structured
    markdown summaries even when external LLM API tokens are not present.
    """
    from ai_explainer import explain_quarantine

    mock_summary = {
        "dataset": "orders",
        "total_quarantined": 15,
        "primary_reasons": ["negative_unit_price", "unreconciled_payment", "missing_store_fk"],
    }

    explanation = explain_quarantine(
        quarantine_summary=mock_summary,
        pipeline_name="NammaMart ETL Shubham Test",
        passed_count=2326,
        failed_count=86,
    )
    assert isinstance(explanation, str)
    assert len(explanation) > 50
    assert "Data Quality Alert" in explanation or "Quarantine" in explanation or "NammaMart" in explanation


def test_slack_notifier_safety():
    """
    Verifies that send_slack_alert safely degrades to console logging without crashing
    when no webhook URL is configured.
    """
    from slack_notifier import send_slack_alert

    # Should not raise exception
    send_slack_alert("Test alert from pytest suite", webhook_url=None)
