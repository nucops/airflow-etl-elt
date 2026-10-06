# NammaMart Quick Commerce — ETL & ELT Pipeline Submission

**Student / Engineer**: Shubham (`shubham@cossth.com`)  
**Course**: IIT Roorkee CEC & Futurense · *Data Pipeline to ML Model* (FDAIE Module 2)  
**Ticket Ref**: `AIT-175`

---

## 1. Submission Checklist

- [x] `dags/nammamart_etl.py` and `dags/nammamart_elt.py` parse with no import errors
- [x] Helper code in `include/` (`dq_checks.py`) and SQL files in `include/sql/`
- [x] Data Issues Log (Part 0)
- [x] Answers to Part C (Scheduling & 9:00 AM IST SLA)
- [x] Governance answers (Part E)
- [x] Observability results table (Part F)
- [x] Executive recommendation to CTO (Part G)
- [x] Answers to all six business questions as SQL plus result
- [x] No secrets or unmasked customer PII outside `include/data/`

---

## 2. Part 0: Data Profiling & Issues Log

| # | File | Row Key / ID | Column | Problem Identified | Quality Dimension | Proposed Rule / Fix | Severity |
|---|---|---|---|---|---|---|---|
| 1 | `orders.csv` | Multiple | `delivery_minutes` | Negative or extreme outlier values (>120 mins) | Accuracy | Enforce `8 <= delivery_minutes <= 120` for DELIVERED | Critical |
| 2 | `orders.csv` | Multiple | `order_amount` | Does not match formula `qty * price * (1 - disc)` | Accuracy | Recompute and reject or flag discrepancies | Critical |
| 3 | `orders.csv` | Multiple | `customer_id` | Orphan IDs not present in `customers.csv` | Consistency | Referential integrity check against dimension | Critical |
| 4 | `payments.csv` | Multiple | `payment_mode` | Casing inconsistency (e.g. `upi`, `UPI `) | Validity | Standardize via `UPPER(TRIM(payment_mode))` | Warning |
| 5 | `customers.csv` | Multiple | `email`, `phone` | PII exposed in cleartext | Validity / Privacy | Mask format (`pr***@example.com`, `98******21`) | Critical |
| 6 | `products.csv` | Multiple | `cost_price`, `mrp` | Negative or `cost_price > mrp` | Accuracy | Validate `0 < cost_price <= mrp` | Critical |
| 7 | `stores.csv` | Multiple | `pincode` | Non-6 digit pincode entries | Validity | Regex `^[1-9][0-9]{5}$` validation | Warning |

### Clarifying Questions for Client
1. *Is a negative quantity considered a return, a customer cancellation, or an upstream data-entry glitch?*
2. *When an order status transitions to RETURNED after multiple days, should financial reports retroactively adjust the order date's revenue or post a negative adjustment on the return date?*
3. *If payment gateway records show SUCCESS but the order amount differs by fractional paise/cents due to rounding, should the transaction be quarantined or reconciled with a rounding tolerance?*

---

## 3. Part C: Scheduling (9:00 AM IST SLA)

* **Cron Expression**: `0 9 * * 1-6` (Monday through Saturday at 09:00 IST).
* **Timezone Configuration**: Explicitly pinned to `Asia/Kolkata` (`start_date=pendulum.datetime(..., tz=pendulum.timezone('Asia/Kolkata'))`).
* **Catchup & Retries**: `catchup=False` to prevent backfill storms; 1 retry with exponential backoff of 2 minutes to handle transient lock/I/O errors.

---

## 4. Part E: Data Governance & Compliance

1. **Ownership**: OMS team owns orders, Finance owns payments, CRM team owns customers, Product Merchandising owns products. Quality rule changes require joint Sign-off between Ops DRI and Lead Data Engineer.
2. **Personal Data & DPDP Act 2023**: Indian Digital Personal Data Protection Act requires masking of Identifiable Customer Data (email, phone, home address). Data is masked rather than dropped so customer analytics and spend tiers remain linkable without exposing PII.
3. **Lineage**: Raw -> Staging -> Mart lineage is tracked via explicit metadata columns (`_loaded_at`, `_run_id`, `_source_file`).
4. **Access Control (RBAC)**: Only compliance auditors can access Raw; Analytics & BI access Staging and Marts; Ops review sees Marts and Quarantine.

---

## 5. Part F: Observability & Failure Injection Experiments

| # | Experiment | Expected | Observed | Verification Location | Action / Mitigation |
|---|---|---|---|---|---|
| 1 | Happy Path | All tasks green, SLA met | Smooth execution across parallel tasks | Airflow Graph & Task Logs | Baseline established |
| 2 | Idempotency | Fact row counts identical | Row counts preserved on rerun | `pipeline_runs` table | Merge/truncate pattern validated |
| 3 | Incremental Load | 150 inserts, 10 status updates | Statuses updated without duplicates | DuckDB `stg_orders` | Merge logic confirmed |
| 4 | Missing File | Task retries and alerts | Task enters `up_for_retry` then fails | Airflow UI Task Details | Added file existence pre-check |
| 5 | Bad Data Injection | Corrupt rows quarantined | Rows moved to `include/quarantine/` | Quarantine CSV output | Rule engine caught all 5 flaws |
| 6 | Schema Drift | Immediate hard stop | Task fails with schema mismatch | Airflow Scheduler log | Schema contract validation |
| 7 | Volume Anomaly | Alert on >90% drop | Warning logged in `dq_results` | `dq_results` table | Configured threshold alert |
| 8 | Distribution Drift | Outlier flagged | Detected in accuracy check | Metric anomaly log | Added Z-score / IQR check |
| 9 | Freshness SLA | Warn on stale data | Flagged by timeliness dimension | Task output log | Strict freshness sensor |
| 10 | Task Recovery | Only downstream re-runs | "Clear" restarts failed node | Airflow Task Tree | Recovery verified |

---

## 6. Part G: Recommendation to CTO (Sameer)

**Recommendation**: Adopt a **Hybrid ELT-first architecture with Staged Validation Gates**.
* **Rationale**: ELT retains untouched raw data for compliance, historical auditing, and data science exploration. By introducing a rigorous Staging validation layer with automated Quarantine tables, the finance team's requirement of 100% clean numbers on executive marts is fully met without discarding the raw lineage.
* **Top 3 Risks**:
  1. Storage growth of raw audit tables over multi-year horizons (Mitigate with retention partitioning).
  2. Late-arriving dimensional changes requiring complex SCD Type 2 handling.
  3. Increased warehouse compute during peak morning transformation windows.
