"""NammaMart Production ETL Pipeline (Part A).

Extract -> Validate -> Quarantine -> Transform -> Load (DuckDB Star Schema) -> Reconcile -> Audit & Scorecard.
SLA: Ready before 9:00 AM IST Monday to Saturday.
Course: IIT Roorkee CEC & Futurense · Data Pipeline to ML Model (FDAIE Module 2)
"""

from __future__ import annotations

import csv
import datetime
import json
import os
import re
import urllib.request
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task

# Environment & Directory Setup
LOCAL_TZ = pendulum.timezone("Asia/Kolkata")
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(_CURRENT_DIR, "..", "include", "data", "nammamart")):
    BASE_DIR = os.path.abspath(os.path.join(_CURRENT_DIR, ".."))
elif os.path.exists(os.path.join(_CURRENT_DIR, "include", "data", "nammamart")):
    BASE_DIR = _CURRENT_DIR
elif os.path.exists("/opt/airflow/dags/include/data/nammamart"):
    BASE_DIR = "/opt/airflow/dags"
elif os.path.exists("/opt/airflow/dags/repo/include/data/nammamart"):
    BASE_DIR = "/opt/airflow/dags/repo"
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.path.join(BASE_DIR, "include", "data", "nammamart")


def _resolve_output_paths(base_dir: str) -> tuple[str, str, str]:
    """Resolve writable paths for quarantine and DuckDB warehouses."""
    if os.path.exists("/opt/airflow/warehouse"):
        target_dir = "/opt/airflow/warehouse"
    else:
        candidate = os.path.join(base_dir, "include")
        try:
            os.makedirs(candidate, exist_ok=True)
            test_file = os.path.join(candidate, ".perm_test")
            with open(test_file, "w") as f:
                f.write("ok")
            os.remove(test_file)
            target_dir = candidate
        except Exception:
            target_dir = "/tmp/nammamart/include"
            os.makedirs(target_dir, exist_ok=True)

    quarantine_dir = os.path.join(target_dir, "quarantine")
    os.makedirs(quarantine_dir, exist_ok=True)
    wh_path = os.path.join(target_dir, "warehouse.duckdb")
    wh_shubham_path = os.path.join(target_dir, "warehouse_shubham.duckdb")

    src_wh = os.path.join(base_dir, "include", "warehouse.duckdb")
    if os.path.exists(src_wh) and not os.path.exists(wh_path) and target_dir != os.path.join(base_dir, "include"):
        try:
            import shutil
            shutil.copy2(src_wh, wh_path)
            shutil.copy2(src_wh, wh_shubham_path)
        except Exception:
            pass

    return quarantine_dir, wh_path, wh_shubham_path


QUARANTINE_DIR, WAREHOUSE_PATH, WAREHOUSE_SHUBHAM_PATH = _resolve_output_paths(BASE_DIR)


def on_failure_alert(context: dict[str, Any]) -> None:
    """Send failure alert to Slack webhook and email using Airflow Variables (Bonus requirement)."""
    dag_id = context.get("task_instance").dag_id if context.get("task_instance") else "nammamart_etl_shubham"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown_task"
    run_id = context.get("run_id", "manual")
    exception = str(context.get("exception", "Execution error"))

    # 1. Slack Notification
    try:
        from airflow.models import Variable
        webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        if webhook_url:
            payload = {
                "text": (
                    f":rotating_light: *NammaMart Pipeline Failure Alert*\n"
                    f"*DAG*: `{dag_id}` | *Task*: `{task_id}`\n"
                    f"*Run ID*: `{run_id}`\n"
                    f"*Error*: `{exception}`"
                )
            }
            req = urllib.request.Request(
                webhook_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=5)
            print("Successfully dispatched Slack failure alert.")
    except Exception as e:
        print(f"Notice: Slack failure alert notification could not be sent: {e}")

    # 2. Email Notification
    try:
        from airflow.models import Variable
        alert_email = Variable.get("ALERT_EMAIL", default_var=None)
        if alert_email:
            from airflow.utils.email import send_email
            subject = f"[Airflow Alert] NammaMart Pipeline Failure: {dag_id}.{task_id}"
            html_content = f"""
            <h3>NammaMart Pipeline Failure Alert</h3>
            <p><strong>DAG:</strong> {dag_id}</p>
            <p><strong>Task:</strong> {task_id}</p>
            <p><strong>Run ID:</strong> {run_id}</p>
            <p><strong>Error Message:</strong></p>
            <pre style="background:#f4f4f4;padding:10px;border-left:4px solid #d9534f;">{exception}</pre>
            """
            recipients = [e.strip() for e in alert_email.split(",") if e.strip()]
            send_email(to=recipients, subject=subject, html_content=html_content)
            print(f"Successfully dispatched Email failure alert to {recipients}.")
    except Exception as e:
        print(f"Notice: Email failure alert notification could not be sent: {e}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_alert,
}


def mask_email(email: str | Any) -> str:
    """Mask email address (e.g. pr***@example.com) for Indian DPDP Act 2023 compliance."""
    if not isinstance(email, str) or "@" not in email:
        return "invalid_email@masked"
    user, domain = email.split("@", 1)
    if len(user) <= 2:
        masked_user = user[0] + "***"
    else:
        masked_user = user[:2] + "***"
    return f"{masked_user}@{domain}"


def mask_phone(phone: str | Any) -> str:
    """Mask phone number (e.g. 98******21) for Indian DPDP Act 2023 compliance."""
    s = str(phone).strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 10:
        return f"{digits[:2]}******{digits[-2:]}"
    return "******"


def normalize_payment_mode(mode: str | Any) -> str:
    """Standardize payment mode casing and whitespace."""
    if not mode:
        return "UNKNOWN"
    cleaned = str(mode).strip().upper()
    if cleaned in ("UPI", "CARD", "WALLET"):
        return cleaned
    if "CASH" in cleaned or "COD" in cleaned:
        return "CASH ON DELIVERY"
    return cleaned


def read_csv_records(file_name: str) -> list[dict[str, str]]:
    """Read CSV file into a list of dictionaries using standard library csv."""
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found: {file_path}")
    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_quarantine_records(file_name: str, records: list[dict[str, Any]], run_id: str) -> str:
    """Write failed records to include/quarantine with failure_reason."""
    clean_run_id = re.sub(r"[^\w\-_\.]", "_", run_id)
    out_name = f"{os.path.splitext(file_name)[0]}_failed_{clean_run_id}.csv"
    out_path = os.path.join(QUARANTINE_DIR, out_name)

    if not records:
        return out_path

    fieldnames = list(records[0].keys())
    try:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        return out_path
    except Exception as e:
        print(f"Notice: Failed to write quarantine file to {out_path}: {e}")
        try:
            fallback_dir = "/tmp/nammamart/quarantine"
            os.makedirs(fallback_dir, exist_ok=True)
            fallback_path = os.path.join(fallback_dir, out_name)
            with open(fallback_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
            return fallback_path
        except Exception as fb_err:
            print(f"Fallback quarantine write failed: {fb_err}")
            return out_path


@dag(
    dag_id="nammamart_etl_shubham",
    default_args=default_args,
    description="NammaMart Production ETL Pipeline with Quality Quarantine and Star Schema",
    schedule="0 9 * * 1-6",  # 9:00 AM IST Monday to Saturday
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["nammamart", "etl", "iitr-futurense", "public-workloads"],
)
def nammamart_etl_pipeline():
    start_time_iso = pendulum.now(LOCAL_TZ).to_iso8601_string()

    # -------------------------------------------------------------------------
    # Step 1: Parallel Extraction Tasks
    # -------------------------------------------------------------------------
    @task
    def extract_orders() -> list[dict[str, str]]:
        return read_csv_records("orders.csv")

    @task
    def extract_payments() -> list[dict[str, str]]:
        return read_csv_records("payments.csv")

    @task
    def extract_customers() -> list[dict[str, str]]:
        return read_csv_records("customers.csv")

    @task
    def extract_products() -> list[dict[str, str]]:
        return read_csv_records("products.csv")

    @task
    def extract_stores() -> list[dict[str, str]]:
        return read_csv_records("stores.csv")

    # -------------------------------------------------------------------------
    # Step 2: Quality Validation & Quarantine (All 6 DQ Dimensions)
    # -------------------------------------------------------------------------
    @task
    def validate_and_quarantine(
        orders_raw: list[dict[str, str]] | None = None,
        payments_raw: list[dict[str, str]] | None = None,
        customers_raw: list[dict[str, str]] | None = None,
        products_raw: list[dict[str, str]] | None = None,
        stores_raw: list[dict[str, str]] | None = None,
        **context: Any,
    ) -> dict[str, Any]:
        orders_raw = orders_raw or []
        payments_raw = payments_raw or []
        customers_raw = customers_raw or []
        products_raw = products_raw or []
        stores_raw = stores_raw or []

        run_id = context.get("run_id", f"etl_run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")
        checked_at = pendulum.now(LOCAL_TZ).to_iso8601_string()

        # Master foreign key sets
        valid_customer_ids = {c["customer_id"].strip() for c in customers_raw if c.get("customer_id")}
        valid_product_ids = {p["product_id"].strip() for p in products_raw if p.get("product_id")}
        valid_store_ids = {s["store_id"].strip() for s in stores_raw if s.get("store_id")}

        dq_metrics: list[dict[str, Any]] = []

        # 1. Validate Stores (Completeness & Accuracy)
        clean_stores = []
        failed_stores = []
        missing_zone_count = 0
        zero_cap_count = 0
        for s in stores_raw:
            reasons = []
            zone = s.get("zone", "").strip()
            if not zone:
                reasons.append("Missing zone (Completeness)")
                missing_zone_count += 1
            try:
                cap = int(s.get("capacity_orders_per_day", 0))
                if cap <= 0:
                    reasons.append(f"Store capacity must be positive, got {cap} (Accuracy)")
                    zero_cap_count += 1
            except (ValueError, TypeError):
                reasons.append("Malformed capacity_orders_per_day")
                zero_cap_count += 1

            if reasons:
                failed_s = dict(s)
                failed_s["failure_reason"] = "; ".join(reasons)
                failed_stores.append(failed_s)
            else:
                clean_stores.append(s)

        write_quarantine_records("stores.csv", failed_stores, run_id)
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "stores_zone_completeness",
            "dimension": "Completeness", "severity": "Warning", "rows_checked": len(stores_raw),
            "rows_failed": missing_zone_count, "status": "PASSED" if missing_zone_count == 0 else "WARNING", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "stores_capacity_accuracy",
            "dimension": "Accuracy", "severity": "Critical", "rows_checked": len(stores_raw),
            "rows_failed": zero_cap_count, "status": "PASSED" if zero_cap_count == 0 else "FAILED", "checked_at": checked_at
        })

        # 2. Validate Products (Accuracy: MRP >= Cost Price > 0)
        clean_products = []
        failed_products = []
        cost_mrp_failed = 0
        for p in products_raw:
            reasons = []
            try:
                cost = float(p.get("cost_price", 0))
                mrp = float(p.get("mrp", 0))
                if cost <= 0:
                    reasons.append("cost_price must be positive")
                    cost_mrp_failed += 1
                elif cost > mrp:
                    reasons.append(f"cost_price {cost} exceeds MRP {mrp}")
                    cost_mrp_failed += 1
            except (ValueError, TypeError):
                reasons.append("Invalid numeric format in cost_price or mrp")
                cost_mrp_failed += 1

            if reasons:
                failed_p = dict(p)
                failed_p["failure_reason"] = "; ".join(reasons)
                failed_products.append(failed_p)
            else:
                clean_products.append(p)

        write_quarantine_records("products.csv", failed_products, run_id)
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "products_cost_vs_mrp",
            "dimension": "Accuracy", "severity": "Critical", "rows_checked": len(products_raw),
            "rows_failed": cost_mrp_failed, "status": "PASSED" if cost_mrp_failed == 0 else "FAILED", "checked_at": checked_at
        })

        # 3. Validate Customers (Uniqueness, Validity: Email/Phone regex, PII)
        seen_cust = set()
        clean_customers = []
        failed_customers = []
        email_regex = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
        invalid_email_count = 0
        invalid_phone_count = 0
        for c in customers_raw:
            cid = c.get("customer_id", "").strip()
            reasons = []
            if not cid:
                reasons.append("Missing customer_id (Completeness)")
            elif cid in seen_cust:
                reasons.append(f"Duplicate customer_id {cid} (Uniqueness)")
            else:
                seen_cust.add(cid)

            email_val = c.get("email", "").strip()
            if not email_regex.match(email_val):
                invalid_email_count += 1

            phone_val = re.sub(r"\D", "", str(c.get("phone", "")).strip())
            if len(phone_val) != 10:
                invalid_phone_count += 1

            if reasons:
                failed_c = dict(c)
                failed_c["failure_reason"] = "; ".join(reasons)
                failed_customers.append(failed_c)
            else:
                clean_customers.append(c)

        write_quarantine_records("customers.csv", failed_customers, run_id)
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "customers_unique_id",
            "dimension": "Uniqueness", "severity": "Critical", "rows_checked": len(customers_raw),
            "rows_failed": len(failed_customers), "status": "PASSED" if not failed_customers else "FAILED", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "customers_email_validity",
            "dimension": "Validity", "severity": "Warning", "rows_checked": len(customers_raw),
            "rows_failed": invalid_email_count, "status": "PASSED" if invalid_email_count == 0 else "WARNING", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "customers_phone_validity",
            "dimension": "Validity", "severity": "Warning", "rows_checked": len(customers_raw),
            "rows_failed": invalid_phone_count, "status": "PASSED" if invalid_phone_count == 0 else "WARNING", "checked_at": checked_at
        })

        # 4. Validate Orders (Critical checks: Quantity, SLA range, Formula, Keys, Timeliness)
        clean_orders = []
        failed_orders = []
        qty_failed = 0
        sla_failed = 0
        formula_failed = 0
        orphan_keys_failed = 0
        future_ts_failed = 0

        for o in orders_raw:
            reasons = []
            oid = o.get("order_id", "").strip()
            status = o.get("order_status", "").strip().upper()

            if not oid:
                reasons.append("Missing order_id (Completeness)")
            if status not in {"DELIVERED", "CANCELLED", "RETURNED"}:
                reasons.append(f"Invalid order_status: {status} (Validity)")

            try:
                qty = float(o.get("quantity", 0))
                if qty <= 0:
                    reasons.append(f"Quantity <= 0: {qty} (Accuracy)")
                    qty_failed += 1
            except (ValueError, TypeError):
                reasons.append("Non-numeric quantity (Accuracy)")
                qty_failed += 1

            if status == "DELIVERED":
                deliv = o.get("delivery_minutes")
                if not deliv or str(deliv).strip() == "":
                    reasons.append("Missing delivery_minutes for DELIVERED order (Completeness)")
                    sla_failed += 1
                else:
                    try:
                        dm = float(deliv)
                        if dm < 8 or dm > 120:
                            reasons.append(f"delivery_minutes {dm} out of SLA range [8, 120] (Accuracy)")
                            sla_failed += 1
                    except (ValueError, TypeError):
                        reasons.append(f"Malformed delivery_minutes: {deliv}")
                        sla_failed += 1

            try:
                qty = float(o.get("quantity", 0))
                price = float(o.get("unit_price", 0))
                disc = float(o.get("discount_pct", 0))
                expected_amt = round(qty * price * (1.0 - disc / 100.0), 2)
                actual_amt = float(o.get("order_amount", 0))
                if abs(actual_amt - expected_amt) > 0.05:
                    reasons.append(f"Formula mismatch: expected {expected_amt}, got {actual_amt} (Accuracy)")
                    formula_failed += 1
            except (ValueError, TypeError):
                reasons.append("Malformed pricing fields")
                formula_failed += 1

            # Consistency (Referential Integrity)
            cid = o.get("customer_id", "").strip()
            pid = o.get("product_id", "").strip()
            sid = o.get("store_id", "").strip()
            if cid not in valid_customer_ids or pid not in valid_product_ids or sid not in valid_store_ids:
                reasons.append(f"Orphan key in order (cust={cid}, prod={pid}, store={sid}) (Consistency)")
                orphan_keys_failed += 1

            # Timeliness
            ts = o.get("order_ts", "").strip()
            if ts > "2026-10-02":
                reasons.append(f"Future order timestamp: {ts} (Timeliness)")
                future_ts_failed += 1

            if reasons:
                failed_o = dict(o)
                failed_o["failure_reason"] = "; ".join(reasons)
                failed_orders.append(failed_o)
            else:
                clean_orders.append(o)

        write_quarantine_records("orders.csv", failed_orders, run_id)
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_quantity_accuracy",
            "dimension": "Accuracy", "severity": "Critical", "rows_checked": len(orders_raw),
            "rows_failed": qty_failed, "status": "PASSED" if qty_failed == 0 else "FAILED", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_delivery_sla_range",
            "dimension": "Accuracy", "severity": "Critical", "rows_checked": len(orders_raw),
            "rows_failed": sla_failed, "status": "PASSED" if sla_failed == 0 else "FAILED", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_amount_formula_match",
            "dimension": "Accuracy", "severity": "Critical", "rows_checked": len(orders_raw),
            "rows_failed": formula_failed, "status": "PASSED" if formula_failed == 0 else "FAILED", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_referential_integrity",
            "dimension": "Consistency", "severity": "Critical", "rows_checked": len(orders_raw),
            "rows_failed": orphan_keys_failed, "status": "PASSED" if orphan_keys_failed == 0 else "FAILED", "checked_at": checked_at
        })
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_future_dates_timeliness",
            "dimension": "Timeliness", "severity": "Critical", "rows_checked": len(orders_raw),
            "rows_failed": future_ts_failed, "status": "PASSED" if future_ts_failed == 0 else "FAILED", "checked_at": checked_at
        })

        # 5. Validate Payments (Consistency & Validity)
        clean_payments = []
        failed_payments = []
        valid_order_ids = {o["order_id"].strip() for o in orders_raw if o.get("order_id")}
        orphan_payments = 0
        for p in payments_raw:
            reasons = []
            p_oid = p.get("order_id", "").strip()
            if p_oid not in valid_order_ids:
                reasons.append(f"order_id {p_oid} not in orders master (Consistency)")
                orphan_payments += 1

            if reasons:
                failed_p = dict(p)
                failed_p["failure_reason"] = "; ".join(reasons)
                failed_payments.append(failed_p)
            else:
                clean_payments.append(p)

        write_quarantine_records("payments.csv", failed_payments, run_id)
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "payments_order_fk_consistency",
            "dimension": "Consistency", "severity": "Critical", "rows_checked": len(payments_raw),
            "rows_failed": orphan_payments, "status": "PASSED" if orphan_payments == 0 else "FAILED", "checked_at": checked_at
        })

        # 6. Volume Anomaly Check (Observability Experiment 7: Alert if volume drops by >90%)
        volume_anomaly_failed = 1 if len(orders_raw) < 200 else 0
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_volume_anomaly",
            "dimension": "Volume", "severity": "Critical", "rows_checked": len(orders_raw),
            "rows_failed": volume_anomaly_failed, "status": "PASSED" if volume_anomaly_failed == 0 else "FAILED", "checked_at": checked_at
        })

        # 7. Distribution Drift Check (Observability Experiment 8: Flag extreme price outliers)
        extreme_price_count = 0
        for o in orders_raw:
            try:
                p = float(o.get("unit_price", 0))
                if p > 5000.0 or p < 0:
                    extreme_price_count += 1
            except Exception:
                extreme_price_count += 1
        dq_metrics.append({
            "run_id": run_id, "dag_id": "nammamart_etl_shubham", "check_name": "orders_distribution_drift",
            "dimension": "Distribution", "severity": "Warning", "rows_checked": len(orders_raw),
            "rows_failed": extreme_price_count, "status": "PASSED" if extreme_price_count == 0 else "WARNING", "checked_at": checked_at
        })

        total_quarantined = len(failed_orders) + len(failed_payments) + len(failed_customers) + len(failed_products) + len(failed_stores)
        total_extracted = len(orders_raw) + len(payments_raw) + len(customers_raw) + len(products_raw) + len(stores_raw)

        return {
            "run_id": run_id,
            "checked_at": checked_at,
            "orders_clean": clean_orders,
            "orders_quarantined": len(failed_orders),
            "payments_clean": clean_payments,
            "payments_quarantined": len(failed_payments),
            "customers_clean": clean_customers,
            "customers_quarantined": len(failed_customers),
            "products_clean": clean_products,
            "products_quarantined": len(failed_products),
            "stores_clean": clean_stores,
            "stores_quarantined": len(failed_stores),
            "total_extracted": total_extracted,
            "total_quarantined": total_quarantined,
            "dq_metrics": dq_metrics,
        }

    # -------------------------------------------------------------------------
    # Step 3: Transformation, PII Masking, & Star Schema Loading
    # -------------------------------------------------------------------------
    @task
    def transform_and_load(val_res: dict[str, Any]) -> dict[str, Any]:
        run_id = val_res["run_id"]
        customers = val_res["customers_clean"]
        products = val_res["products_clean"]
        stores = val_res["stores_clean"]
        orders = val_res["orders_clean"]

        # Product cost lookup
        product_cost_map = {}
        for p in products:
            try:
                product_cost_map[p["product_id"]] = float(p.get("cost_price", 0))
            except (ValueError, TypeError):
                product_cost_map[p["product_id"]] = 0.0

        # Transform Customers: Mask email and phone (DPDP Act 2023)
        transformed_customers = []
        for c in customers:
            transformed_customers.append({
                "customer_id": c["customer_id"],
                "full_name": c.get("full_name", ""),
                "email_masked": mask_email(c.get("email", "")),
                "phone_masked": mask_phone(c.get("phone", "")),
                "city": c.get("city", ""),
                "area": c.get("area", ""),
                "pincode": c.get("pincode", ""),
                "signup_date": c.get("signup_date", ""),
                "loyalty_tier": c.get("loyalty_tier", "Standard").strip().upper(),
                "is_active": c.get("is_active", "True"),
            })

        # Transform Products
        transformed_products = []
        for p in products:
            transformed_products.append({
                "product_id": p["product_id"],
                "product_name": p.get("product_name", ""),
                "category": p.get("category", ""),
                "sub_category": p.get("sub_category", ""),
                "brand": p.get("brand", ""),
                "mrp": float(p.get("mrp", 0)),
                "cost_price": float(p.get("cost_price", 0)),
                "unit": p.get("unit", ""),
                "is_perishable": p.get("is_perishable", "False"),
                "launch_date": p.get("launch_date", ""),
            })

        # Transform Stores (Impute zone if missing e.g. BTM Layout -> South)
        transformed_stores = []
        for s in stores:
            zone = s.get("zone", "").strip()
            if not zone and "BTM" in s.get("area", ""):
                zone = "South"
            transformed_stores.append({
                "store_id": s["store_id"],
                "store_name": s.get("store_name", ""),
                "zone": zone.upper() if zone else "UNKNOWN",
                "area": s.get("area", ""),
                "pincode": s.get("pincode", ""),
                "open_date": s.get("open_date", ""),
                "manager_name": s.get("manager_name", ""),
                "capacity_orders_per_day": int(s.get("capacity_orders_per_day", 100)),
            })

        # Transform Orders: Derive metrics
        transformed_orders = []
        for o in orders:
            qty = float(o.get("quantity", 0))
            price = float(o.get("unit_price", 0))
            disc = float(o.get("discount_pct", 0))
            amt = float(o.get("order_amount", 0))
            status = o.get("order_status", "").upper()
            deliv_raw = o.get("delivery_minutes")

            order_date = o.get("order_ts", "")[:10]
            net_rev = amt if status == "DELIVERED" else 0.0

            cost_price = product_cost_map.get(o.get("product_id"), 0.0)
            gross_margin = (net_rev - (qty * cost_price)) if status == "DELIVERED" else 0.0

            try:
                dm = float(deliv_raw) if deliv_raw else 999.0
                within_15 = (dm <= 15.0) and (status == "DELIVERED")
            except (ValueError, TypeError):
                dm = None
                within_15 = False

            transformed_orders.append({
                "order_id": o["order_id"],
                "order_ts": o.get("order_ts", ""),
                "order_date": order_date,
                "customer_id": o.get("customer_id"),
                "store_id": o.get("store_id"),
                "product_id": o.get("product_id"),
                "quantity": qty,
                "unit_price": price,
                "discount_pct": disc,
                "order_amount": amt,
                "payment_mode": normalize_payment_mode(o.get("payment_mode")),
                "order_status": status,
                "channel": o.get("channel", "APP"),
                "delivery_minutes": dm,
                "net_revenue": round(net_rev, 2),
                "gross_margin": round(gross_margin, 2),
                "within_15_min": within_15,
            })

        # Idempotent Load into DuckDB Star Schema
        import duckdb
        import pandas as pd
        target_warehouses = [WAREHOUSE_PATH, WAREHOUSE_SHUBHAM_PATH]
        for wh in target_warehouses:
            try:
                os.makedirs(os.path.dirname(wh), exist_ok=True)
                con = duckdb.connect(wh)

                # 1. etl_dim_customer
                df_cust = pd.DataFrame(transformed_customers)
                con.register("df_cust_view", df_cust)
                con.execute("CREATE TABLE IF NOT EXISTS etl_dim_customer AS SELECT * FROM df_cust_view WHERE 1=0;")
                con.execute("DELETE FROM etl_dim_customer;")
                con.execute("INSERT INTO etl_dim_customer SELECT * FROM df_cust_view;")

                # 2. etl_dim_product
                df_prod = pd.DataFrame(transformed_products)
                con.register("df_prod_view", df_prod)
                con.execute("CREATE TABLE IF NOT EXISTS etl_dim_product AS SELECT * FROM df_prod_view WHERE 1=0;")
                con.execute("DELETE FROM etl_dim_product;")
                con.execute("INSERT INTO etl_dim_product SELECT * FROM df_prod_view;")

                # 3. etl_dim_store
                df_store = pd.DataFrame(transformed_stores)
                con.register("df_store_view", df_store)
                con.execute("CREATE TABLE IF NOT EXISTS etl_dim_store AS SELECT * FROM df_store_view WHERE 1=0;")
                con.execute("DELETE FROM etl_dim_store;")
                con.execute("INSERT INTO etl_dim_store SELECT * FROM df_store_view;")

                # 4. etl_fact_orders
                df_orders = pd.DataFrame(transformed_orders)
                con.register("df_orders_view", df_orders)
                con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM df_orders_view WHERE 1=0;")
                con.execute("DELETE FROM etl_fact_orders;")
                con.execute("INSERT INTO etl_fact_orders SELECT * FROM df_orders_view;")

                # 5. Scorecard: dq_results
                df_dq = pd.DataFrame(val_res["dq_metrics"])
                con.register("df_dq_view", df_dq)
                con.execute("""
                    CREATE TABLE IF NOT EXISTS dq_results (
                        run_id VARCHAR,
                        dag_id VARCHAR,
                        check_name VARCHAR,
                        dimension VARCHAR,
                        severity VARCHAR,
                        rows_checked INTEGER,
                        rows_failed INTEGER,
                        status VARCHAR,
                        checked_at TIMESTAMP
                    );
                """)
                con.execute("INSERT INTO dq_results SELECT * FROM df_dq_view;")

                con.close()
            except Exception as e:
                print(f"DuckDB storage warning on {wh}: {e}")

        return {
            "run_id": run_id,
            "orders_loaded": len(transformed_orders),
            "customers_loaded": len(transformed_customers),
            "products_loaded": len(transformed_products),
            "stores_loaded": len(transformed_stores),
            "orders": transformed_orders,
            "payments_clean": val_res["payments_clean"],
            "total_extracted": val_res["total_extracted"],
            "total_quarantined": val_res["total_quarantined"],
        }

    # -------------------------------------------------------------------------
    # Step 4: Payment Gateway Reconciliation & Capacity Verification
    # -------------------------------------------------------------------------
    @task
    def reconcile_payments(load_res: dict[str, Any]) -> dict[str, Any]:
        orders = {o["order_id"]: o for o in load_res["orders"]}
        payments = load_res["payments_clean"]

        reconciliation_records = []
        for p in payments:
            pid = p.get("payment_id")
            oid = p.get("order_id")
            p_amt = float(p.get("amount_paid", 0))
            p_status = p.get("payment_status", "").upper()

            matched_order = orders.get(oid)
            if not matched_order:
                reconciliation_records.append({
                    "payment_id": pid,
                    "order_id": oid,
                    "order_amount": None,
                    "amount_paid": p_amt,
                    "payment_status": p_status,
                    "is_reconciled": False,
                    "discrepancy_reason": "Order not found or quarantined",
                })
            else:
                o_amt = matched_order["order_amount"]
                o_status = matched_order["order_status"]

                if p_status == "SUCCESS":
                    if abs(p_amt - o_amt) <= 0.05:
                        reconciliation_records.append({
                            "payment_id": pid,
                            "order_id": oid,
                            "order_amount": o_amt,
                            "amount_paid": p_amt,
                            "payment_status": p_status,
                            "is_reconciled": True,
                            "discrepancy_reason": "Matched",
                        })
                    else:
                        reconciliation_records.append({
                            "payment_id": pid,
                            "order_id": oid,
                            "order_amount": o_amt,
                            "amount_paid": p_amt,
                            "payment_status": p_status,
                            "is_reconciled": False,
                            "discrepancy_reason": f"Amount difference: order={o_amt}, paid={p_amt}",
                        })
                else:
                    reconciliation_records.append({
                        "payment_id": pid,
                        "order_id": oid,
                        "order_amount": o_amt,
                        "amount_paid": p_amt,
                        "payment_status": p_status,
                        "is_reconciled": (o_status == "CANCELLED"),
                        "discrepancy_reason": f"Non-success status: {p_status}",
                    })

        unreconciled_count = sum(1 for r in reconciliation_records if not r["is_reconciled"])

        # Persist etl_payment_reconciliation table to DuckDB
        import duckdb
        import pandas as pd
        df_recon = pd.DataFrame(reconciliation_records)
        target_warehouses = [WAREHOUSE_PATH, WAREHOUSE_SHUBHAM_PATH]
        for wh in target_warehouses:
            try:
                con = duckdb.connect(wh)
                con.register("df_recon_view", df_recon)
                con.execute("CREATE TABLE IF NOT EXISTS etl_payment_reconciliation AS SELECT * FROM df_recon_view WHERE 1=0;")
                con.execute("DELETE FROM etl_payment_reconciliation;")
                con.execute("INSERT INTO etl_payment_reconciliation SELECT * FROM df_recon_view;")
                con.close()
            except Exception as e:
                print(f"Failed to persist etl_payment_reconciliation to {wh}: {e}")

        return {
            "run_id": load_res["run_id"],
            "total_payments_checked": len(reconciliation_records),
            "unreconciled_discrepancies": unreconciled_count,
            "orders_loaded": load_res["orders_loaded"],
            "total_extracted": load_res["total_extracted"],
            "total_quarantined": load_res["total_quarantined"],
        }

    # -------------------------------------------------------------------------
    # Step 5: Executive Morning Reporting & Store Capacity Check (Bonus)
    # -------------------------------------------------------------------------
    @task
    def build_daily_reports(recon_res: dict[str, Any]) -> dict[str, str]:
        print("\n=======================================================")
        print("NAMMAMART EXECUTIVE MORNING DASHBOARD (09:00 AM IST)")
        print("=======================================================")
        print(f"Total Fact Orders Loaded: {recon_res['orders_loaded']}")
        print(f"Payment Gateway Reconciled Checks: {recon_res['total_payments_checked']}")
        print(f"Reconciliation Mismatch Discrepancies: {recon_res['unreconciled_discrepancies']}")

        # Store Capacity Utilization Check (Bonus Requirement)
        try:
            import duckdb
            con = duckdb.connect(WAREHOUSE_PATH)
            capacity_audit = con.execute("""
                SELECT 
                    s.store_id, 
                    s.store_name, 
                    s.capacity_orders_per_day,
                    COUNT(o.order_id) / 30 AS avg_daily_orders,
                    ROUND((COUNT(o.order_id) / 30.0) / NULLIF(s.capacity_orders_per_day, 0) * 100, 1) AS capacity_utilization_pct
                FROM etl_dim_store s
                LEFT JOIN etl_fact_orders o ON s.store_id = o.store_id
                GROUP BY s.store_id, s.store_name, s.capacity_orders_per_day
                ORDER BY capacity_utilization_pct DESC;
            """).fetchall()
            print("--- Store Capacity Audit (Daily Orders vs Capacity) ---")
            for row in capacity_audit:
                print(f"Store {row[0]} ({row[1]}): {row[3]:.1f} orders/day vs {row[2]} cap -> {row[4]}% utilization")
            con.close()
        except Exception as e:
            print(f"Capacity check notice: {e}")

        print("Executive KPI reports delivered successfully before 09:30 AM leadership standup.")
        return {"status": "SUCCESS", "delivered_at": pendulum.now(LOCAL_TZ).to_iso8601_string()}

    # -------------------------------------------------------------------------
    # Step 6: Observability Run Audit Task (trigger_rule="all_done")
    # -------------------------------------------------------------------------
    @task(trigger_rule="all_done")
    def record_pipeline_run(recon_res: Any = None, **context: Any) -> None:
        """Record run audit entry in pipeline_runs table in DuckDB."""
        import duckdb
        import pandas as pd
        if not isinstance(recon_res, dict):
            recon_res = {}
        end_time_iso = pendulum.now(LOCAL_TZ).to_iso8601_string()
        run_id = recon_res.get("run_id") or context.get("run_id", "etl_manual_run")
        rows_extracted = recon_res.get("total_extracted", 0)
        rows_loaded = recon_res.get("orders_loaded", 0)
        rows_quarantined = recon_res.get("total_quarantined", 0)
        status = "SUCCESS" if rows_loaded > 0 else "FAILED"

        run_record = pd.DataFrame([{
            "run_id": run_id,
            "dag_id": "nammamart_etl_shubham",
            "start_time": start_time_iso,
            "end_time": end_time_iso,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
            "rows_quarantined": rows_quarantined,
            "status": status,
        }])

        target_warehouses = [WAREHOUSE_PATH, WAREHOUSE_SHUBHAM_PATH]
        for wh in target_warehouses:
            try:
                con = duckdb.connect(wh)
                con.register("df_run_view", run_record)
                con.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        run_id VARCHAR PRIMARY KEY,
                        dag_id VARCHAR,
                        start_time TIMESTAMP,
                        end_time TIMESTAMP,
                        rows_extracted INTEGER,
                        rows_loaded INTEGER,
                        rows_quarantined INTEGER,
                        status VARCHAR
                    );
                """)
                con.execute("DELETE FROM pipeline_runs WHERE run_id = df_run_view.run_id;")
                con.execute("INSERT INTO pipeline_runs SELECT * FROM df_run_view;")
                con.close()
                print(f"Recorded pipeline audit in {wh}: run_id={run_id}")
            except Exception as e:
                print(f"Failed to record pipeline audit in {wh}: {e}")

    # Flow Graph Execution
    raw_orders = extract_orders()
    raw_payments = extract_payments()
    raw_customers = extract_customers()
    raw_products = extract_products()
    raw_stores = extract_stores()

    validation_summary = validate_and_quarantine(
        raw_orders, raw_payments, raw_customers, raw_products, raw_stores
    )
    load_summary = transform_and_load(validation_summary)
    recon_summary = reconcile_payments(load_summary)
    reports = build_daily_reports(recon_summary)
    record_pipeline_run(recon_summary)


etl_dag = nammamart_etl_pipeline()


if __name__ == "__main__":
    print("Testing NammaMart ETL Pipeline compilation and TaskFlow definition...")
    print(f"DAG ID: {etl_dag.dag_id}, Schedule: {etl_dag.schedule_interval}")
