# NammaMart Quick Commerce — ETL & ELT Pipeline Submission

**Student / Engineer**: `<Your Name>` (`<your_email>`)  
**Course**: IIT Roorkee CEC & Futurense · *Data Pipeline to ML Model* (FDAIE Module 2)  
**Ticket Ref**: `AIT-175`  
**Target Repository**: `https://github.com/nucops/airflow-etl-elt`  
**Target Branch**: `solution`  

---

## 1. Submission Checklist

- [ ] `dags/nammamart_etl.py` and `dags/nammamart_elt.py` parse with no import errors (`python -m py_compile` validated).
- [ ] Helper code in `include/` (`dq_checks.py`) and SQL models in `include/sql/staging/` & `include/sql/marts/`.
- [ ] Data Issues Log (Part 0) covering all master datasets with identified issues and 3 client clarifying questions.
- [ ] Answers to Part C (Scheduling, 9:00 AM IST SLA, Pendulum timezone, 03:30 UTC time, UTC default behavior, offline laptop behavior, catchup=False justification).
- [ ] Data quality dimensions (Part D) covering all 6 dimensions with severities; persistent `dq_results` scorecard table populated on every run.
- [ ] Governance answers (Part E) addressing ownership, data dictionary, lineage, DPDP Act 2023, RBAC, retention, secrets, audit, and change history.
- [ ] Observability suite (Part F) with 10 failure injection experiments, 5 Observability Pillars mapping, and `pipeline_runs` run audit table with `trigger_rule="all_done"`.
- [ ] Executive recommendation to CTO Sameer (Part G) comparing ETL vs. ELT with architectural tradeoffs and top 3 risks.
- [ ] Answers to all six business questions as SQL queries plus execution results from **BOTH pipelines**.
- [ ] Bonus features: Slack failure alerting, SCD Type 2 customer dimension, store capacity utilization audit, and DQ trend scorecard.
- [ ] Screenshots captured and linked in Section 10: both DAGs in Graph view (all green), schedule showing 9:00 AM IST, quarantine CSV with failure reasons, and DuckDB SQL query results (`dq_results`, `pipeline_runs`, and `mart_payment_reconciliation`).
- [ ] No secrets, API keys, or cleartext customer PII outside `include/data/`.

---

## 2. Part 0: Data Profiling & Issues Log

| # | File | Row Key / ID Samples | Column | Problem Identified | Quality Dimension | Proposed Rule / Fix | Severity |
|---|---|---|---|---|---|---|---|
| 1 | `orders.csv` | ORD000469, ORD001296 | `quantity` | Quantity <= 0 | Accuracy | Enforce `quantity > 0`; quarantine non-positive | Critical |
| 2 | `orders.csv` | ORD001554, ORD001800 | `delivery_minutes` | Out of SLA (<8 or >120 mins) or empty for DELIVERED | Accuracy / Completeness | Enforce `8 <= delivery_minutes <= 120` | Critical |
| 3 | `orders.csv` | ORD002103, ORD002305 | `order_amount` | Formula mismatch `qty * price * (1 - disc)` | Accuracy | Recalculate; quarantine if mismatch > 0.05 | Critical |
| 4 | `orders.csv` | ORD002255, ORD000701 | `customer_id` | Orphan customer IDs not in `customers.csv` | Consistency | FK referential integrity check | Critical |
| 5 | `orders.csv` | ORD000986, ORD001058 | `product_id` | Orphan product IDs not in `products.csv` | Consistency | FK referential integrity check | Critical |
| 6 | `orders.csv` | ORD001799, ORD001046 | `store_id` | Orphan store IDs not in `stores.csv` | Consistency | FK referential integrity check | Critical |
| 7 | `orders.csv` | ORD002119, ORD000403 | `order_ts` | Future-dated order timestamps | Timeliness | Enforce `order_ts <= current_run_timestamp` | Critical |
| 8 | `payments.csv` | PAY0002188, PAY0001850 | `payment_mode` | Inconsistent casing/spaces (e.g. `'upi'`, `'UPI '`) | Validity | Standardize via `UPPER(TRIM(payment_mode))` | Warning |
| 9 | `payments.csv` | PAY0002219, PAY0002220 | `order_id` | Payment records for non-existent order IDs | Consistency | FK check against orders master | Critical |
| 10 | `payments.csv` | PAY0001568, PAY0000354 | `amount_paid` | SUCCESS payment amount differs from order amount | Accuracy | Enforce payment reconciliation balance equality | Critical |
| 11 | `customers.csv` | C00337, C00307 | `email` | Malformed email addresses | Validity | Regex validation | Warning |
| 12 | `customers.csv` | C00151, C00240 | `phone` | Phone numbers not 10 digits | Validity | 10-digit mobile regex | Warning |
| 13 | `customers.csv` | All records | `email`, `phone` | Cleartext PII exposed | Validity / Privacy | Mask format (`pr***@example.com`, `98******21`) | Critical |
| 14 | `products.csv` | P0019, P0073 | `cost_price` | Cost price exceeds MRP | Accuracy | Enforce `0 < cost_price <= mrp` | Critical |
| 15 | `stores.csv` | S07 | `zone` | Missing/empty zone | Completeness | Reject or impute based on standard area master | Warning |
| 16 | `stores.csv` | S12 | `capacity_orders_per_day` | Store capacity declared as 0 | Accuracy / Validity | Enforce `capacity_orders_per_day > 0` | Critical |
| 17 | `orders_increment` | 10 status corrections | `order_status` | Late status transitions (DELIVERED -> RETURNED) | Consistency | Execute SQL `MERGE` on `order_id` | Critical |

### Clarifying Questions for Client
1. *Is a negative or zero quantity considered a merchandise return, an unfulfilled cancellation, or a POS data-entry glitch?*
2. *When an order status transitions to RETURNED days later in the incremental feed, should the executive morning dashboard retroactively decrease revenue on the original purchase date, or record a separate credit adjustment on the return date?*
3. *If payment gateway records report SUCCESS but the payment amount differs by a few paise due to gateway rounding or platform fees, should the transaction be quarantined or reconciled with a defined financial tolerance window?*

---

## 3. Part C: Scheduling (9:00 AM IST SLA)

* **Cron Expression**: `0 9 * * 1-6` (Monday through Saturday at 09:00 AM IST).
* **Timezone Configuration**: Explicitly configured via Pendulum: `start_date=pendulum.datetime(2026, 9, 1, tz=pendulum.timezone('Asia/Kolkata'))`.
* **Execution Timeout**: 15 minutes hard timeout (`execution_timeout=timedelta(minutes=15)`).
* **Retries & Backoff**: `retries=1`, `retry_delay=timedelta(minutes=2)`.

### Direct Answers to Part C Questions:
1. **What is your cron expression?**  
   `0 9 * * 1-6`
2. **What time does the run show in UTC in the Airflow UI?**  
   **`03:30 AM UTC`** (09:00 IST - 5h30m).
3. **What would happen at 9 AM if you had left the `start_date` in UTC?**  
   The DAG would run at 09:00 UTC (02:30 PM IST), missing the 09:30 AM leadership meeting by 5 hours.
4. **What happens if your laptop or Docker is off at 9 AM?**  
   - With `catchup=False`: The missed run is skipped; Airflow waits for the next scheduled window.
   - Justification: Daily morning reports are point-in-time operational snapshots. Backfilling multiple stale runs creates database contention and risk of overwriting current metrics.

---

## 4. Part E: Data Governance & Compliance

1. **Ownership**: OMS owns orders; Finance owns payments; CRM owns customers; Inventory owns products; Operations owns stores; Data Engineering owns warehouse and marts. Threshold changes require joint VP Ops & Lead Data Engineer sign-off.
2. **Data Dictionary**: Key columns documented with data types, nullability, sources, and PII classifications.
3. **Lineage**: Source row -> DQ Filter -> Staging -> Mart KPI. Tracing in ELT is superior because `raw_*` tables preserve raw records with audit metadata (`_loaded_at`, `_source_file`, `_run_id`).
4. **Personal Data & Indian DPDP Act 2023**: Masked email (`pr***@domain.com`) and phone (`98******21`) to comply with purpose limitation and data minimization under DPDP Act 2023.
5. **Access Control (RBAC)**: Raw layer restricted to Compliance/Admins; Staging to Data Engineers; Marts to BI Analysts, Store Managers, and Executives; Quarantine to Ops Leads.
6. **Retention Policy**: Raw data retained for 7 years (statutory audit); Quarantine retained for 90 days; Masked customer data retained during active lifecycle + 3 years.
7. **Secrets Management**: Stored in Kubernetes Secrets / environment variables; never hardcoded in DAG files or pushed to Git.
8. **Audit Trail for Finance**: Quarantined order proof via `quarantine/*_failed_*.csv` with `failure_reason` and absence from `etl_fact_orders` / `WHERE dq_status = 'PASSED'` in ELT.
9. **Change History & Late-Arriving Returns**: Supported via point-in-time monthly snapshots for Finance Statutory audit alongside SCD Type 2 tracking for operational current-state.

---

## 5. Part F: Observability & Failure Injection Experiments

| # | Experiment | Expected | Observed | Verification Location | Action / Mitigation |
|---|---|---|---|---|---|
| 1 | Happy Path | All tasks green, SLA met before 09:00 AM | Clean execution across parallel extractors | Airflow Graph & Task Logs | Baseline execution verified |
| 2 | Idempotency | Re-trigger produces identical fact counts | Fact counts unchanged; zero duplicate rows | `pipeline_runs` table | Validated truncate/merge pattern |
| 3 | Incremental Load | 150 inserts, 10 status updates | 10 returned orders updated; 0 duplicates | `stg_orders` row count | SQL MERGE logic confirmed |
| 4 | Missing File | Task retries and triggers failure alert | Task enters `up_for_retry` then fails | Airflow Task Details | File presence pre-check sensor |
| 5 | Bad Data Injection | Corrupt rows quarantined with explicit reason | Rows redirected to `include/quarantine/` | Quarantine CSV output | Rule engine caught all flaws |
| 6 | Schema Drift | Immediate hard stop on missing column | Task fails loudly with missing column error | Airflow Scheduler log | Schema contract validation |
| 7 | Volume Anomaly | Alert on >90% drop in expected volume | Warning logged in `dq_results` | `dq_results` scorecard | Threshold anomaly alert |
| 8 | Distribution Drift | Outlier price flagged | Accuracy check flags extreme unit price | Anomaly audit log | Z-score / IQR check added |
| 9 | Freshness SLA | Warn on stale data older than 24h | Timeliness dimension flags stale batch | Task execution output | Strict freshness rule gate |
| 10 | Task Recovery | Downstream re-run from failed node | "Clear" restarts only downstream nodes | Airflow Task Graph | Idempotent task recovery verified |

### Mapping to the 5 Pillars of Data Observability
* **Freshness**: Tracked via DAG schedule (`0 9 * * 1-6`), `execution_timeout`, and run timestamps.
* **Volume**: Tracked via `pipeline_runs` (`rows_extracted`, `rows_loaded`, `rows_quarantined`).
* **Schema**: Enforced via Pydantic/Python in ETL and SQL casting (`TRY_CAST`) in ELT.
* **Distribution**: Enforced via range checks (`quantity > 0`, `mrp >= cost_price`, `8 <= delivery_minutes <= 120`).
* **Lineage**: Tracked via metadata columns (`_loaded_at`, `_source_file`, `_run_id`).

---

## 6. Part G: Recommendation to CTO (Sameer)

**Recommendation: Adopt a Hybrid ELT Architecture with Staged Validation Gates.**

### Executive Rationale
1. **Immutable Auditability**: All raw source rows preserved in `raw_*` tables for regulatory audit.
2. **Schema Resilience**: Column additions in source systems flow through without breaking pipelines.
3. **Clean Decoupling**: Critical quality gates isolated in Staging SQL views while preserving raw history.
4. **Cloud-Native Scalability (1000× Growth)**: Vectorized DuckDB/ClickHouse columnar engines scale gracefully compared to in-memory Python pandas.

### Top 3 Implementation Risks & Mitigations
1. **Raw Storage Growth**: Mitigate with partition pruning and Parquet cold storage after 90 days.
2. **Compute Contention**: Mitigate by allocating dedicated warehouse compute pools for batch ELT.
3. **Late-Arriving Dimension Tracking**: Mitigate with SCD Type 2 dimension models (`dim_customer_scd2`).

---

## 7. Business Questions Evaluation & SQL Solutions

### Question 1: Daily Net Revenue by Zone and Store
```sql
SELECT order_date, zone, store_name, total_orders, delivered_orders, daily_net_revenue, avg_delivery_minutes
FROM mart_daily_store_kpis
ORDER BY order_date DESC, daily_net_revenue DESC
LIMIT 5;
```

### Question 2: Revenue and Gross Margin by Product Category
```sql
SELECT category, delivered_orders, total_units_sold, total_revenue, total_cogs, gross_margin, gross_margin_pct
FROM mart_category_margin
ORDER BY total_revenue DESC;
```

### Question 3: Average Delivery Time per Store & % Within 15 Min
```sql
SELECT store_id, store_name, zone, total_delivered_orders, avg_delivery_minutes, delivered_within_15_min, pct_within_15_min
FROM mart_delivery_sla
ORDER BY total_delivered_orders DESC
LIMIT 5;
```

### Question 4: Revenue by Payment Mode and Unreconciled Amount
```sql
SELECT payment_mode, total_orders, reconciled_revenue, unreconciled_transactions, unreconciled_amount
FROM mart_payment_reconciliation
ORDER BY reconciled_revenue DESC;
```

### Question 5: Top 10 Customers by Spend (Masked PII)
```sql
SELECT customer_id, full_name, email_masked, phone_masked, loyalty_tier, total_orders, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10;
```

### Question 6: Rejected Rows by File and Reason
```sql
SELECT _source_file, dq_status AS failure_reason, COUNT(*) AS rejected_count
FROM stg_orders
WHERE dq_status != 'PASSED'
GROUP BY _source_file, dq_status
ORDER BY rejected_count DESC;
```

---

## 8. Bonus Features Implemented (+10 Marks)

1. **Slack Alert on Failure (`Variable.get("SLACK_WEBHOOK_URL")`)**
2. **SCD Type 2 Customer Dimension (`dim_customer_scd2.sql`)**
3. **Store Capacity Check (`daily_orders` vs `capacity_orders_per_day`)**
4. **Data Quality Longitudinal Trend Scorecard (`dq_results`)**

---

## 9. Observability Tables & Run Audit Evidence

### Table 1: `pipeline_runs`
```sql
SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status
FROM pipeline_runs
ORDER BY start_time DESC;
```

### Table 2: `dq_results`
```sql
SELECT dimension, severity, check_name, rows_checked, rows_failed, status
FROM dq_results
ORDER BY severity DESC, dimension ASC;
```

---

## 10. Visual Verification & Proof of Execution (Screenshots)

Save all captured images into `screenshots/`:

### 10.1 Airflow DAG Graph Views (All Green Successful Runs)
* **ETL Pipeline (`nammamart_etl`)**:
  ![ETL DAG Graph View](screenshots/01_dag_graph_etl.png)

* **ELT Pipeline (`nammamart_elt`)**:
  ![ELT DAG Graph View](screenshots/02_dag_graph_elt.png)

### 10.2 DAG Schedule & 9:00 AM IST SLA
![DAG Schedule 9:00 AM IST](screenshots/03_schedule_9am_ist.png)

### 10.3 Quarantine Output File
![Quarantine File with Failure Reasons](screenshots/04_quarantine_file.png)

### 10.4 Data Quality Scorecard Query (`dq_results`)
![Data Quality Results Query](screenshots/05_dq_results_query.png)

### 10.5 Pipeline Run Audit Query (`pipeline_runs`)
![Pipeline Run Audit Query](screenshots/06_pipeline_runs_query.png)

### 10.6 Payment Reconciliation Query
![Payment Reconciliation Query](screenshots/07_payment_reconciliation.png)

### 10.7 Bonus: Slack Failure Alert Notification (Optional)
![Slack Alert Notification](screenshots/08_slack_failure_alert.png)

---
**Verified & Signed off by**: `<Your Name>` (Forward Deployed Engineer)
