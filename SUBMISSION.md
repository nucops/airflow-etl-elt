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

| # | File | Row Key / ID Samples | Column | Problem Identified | Quality Dimension | Proposed Rule / Fix | Severity |
|---|---|---|---|---|---|---|---|
| 1 | `orders.csv` | ORD000469, ORD001296, ORD001459 | `quantity` | Quantity <= 0 (10 invalid records) | Accuracy | Enforce `quantity > 0` constraint; quarantine non-positive | Critical |
| 2 | `orders.csv` | ORD001554, ORD001800, ORD001037 | `delivery_minutes` | Out of SLA (<8 or >120 mins) or empty for DELIVERED (14 records) | Accuracy / Completeness | Enforce `8 <= delivery_minutes <= 120` for DELIVERED | Critical |
| 3 | `orders.csv` | ORD002103, ORD002305, ORD002004 | `order_amount` | Recomputed formula mismatch `qty * price * (1 - disc)` (18 records) | Accuracy | Recalculate; quarantine if mismatch exceeds 0.05 rounding tolerance | Critical |
| 4 | `orders.csv` | ORD002255, ORD000701, ORD000997 | `customer_id` | Orphan customer IDs not present in `customers.csv` (18 records) | Consistency | Foreign key referential integrity check against customer master | Critical |
| 5 | `orders.csv` | ORD000986, ORD001058, ORD001685 | `product_id` | Orphan product IDs not present in `products.csv` (9 records) | Consistency | Foreign key referential integrity check against product master | Critical |
| 6 | `orders.csv` | ORD001799, ORD001046, ORD001492 | `store_id` | Orphan store IDs not present in `stores.csv` (5 records) | Consistency | Foreign key referential integrity check against store master | Critical |
| 7 | `orders.csv` | ORD002119, ORD000403, ORD001693 | `order_ts` | Future-dated order timestamps after run date (12 records) | Timeliness | Enforce `order_ts <= current_run_timestamp`; quarantine future dates | Critical |
| 8 | `payments.csv` | PAY0002188, PAY0001850, PAY0000817 | `payment_mode` | Inconsistent casing/spaces (e.g. `'upi'`, `'UPI '`) (6 records) | Validity | Standardize via `UPPER(TRIM(payment_mode))` | Warning |
| 9 | `payments.csv` | PAY0002219, PAY0002220, PAY0002223 | `order_id` | Payment records for non-existent order IDs (8 records) | Consistency | Referential integrity check; quarantine orphan gateway transactions | Critical |
| 10 | `payments.csv` | PAY0001568, PAY0000354, PAY0000779 | `amount_paid` | SUCCESS payment amount differs from order amount (14 records) | Accuracy | Enforce payment reconciliation balance equality | Critical |
| 11 | `customers.csv` | C00337, C00307, C00063 | `email` | Malformed email addresses missing domain or @ (12 records) | Validity | Validate with standard email regex `^[\w\.-]+@[\w\.-]+\.\w+$` | Warning |
| 12 | `customers.csv` | C00151, C00240, C00095 | `phone` | Phone numbers not 10 digits or non-numeric (7 records) | Validity | Enforce 10-digit Indian mobile regex `^[6-9][0-9]{9}$` | Warning |
| 13 | `customers.csv` | All records (405 customers) | `email`, `phone` | Cleartext PII exposed violating Indian DPDP Act 2023 | Validity / Privacy | Mask format (`pr***@example.com`, `98******21`) | Critical |
| 14 | `products.csv` | P0019, P0073, P0074 | `cost_price` | Cost price exceeds Maximum Retail Price (MRP) (5 records) | Accuracy | Enforce `0 < cost_price <= mrp`; quarantine negative margin rows | Critical |
| 15 | `orders_increment` | 10 status corrections | `order_status` | Late status transitions (DELIVERED -> RETURNED) requiring upsert | Consistency | Execute SQL `MERGE` / upsert on `order_id` to update status without duplicate rows | Critical |

### Clarifying Questions for Client
1. *Is a negative or zero quantity considered a merchandise return, an unfulfilled cancellation, or a POS data-entry glitch?*
2. *When an order status transitions to RETURNED days later in the incremental feed, should the executive morning dashboard retroactively decrease revenue on the original purchase date, or record a separate credit adjustment on the return date?*
3. *If payment gateway records report SUCCESS but the payment amount differs by a few paise due to gateway rounding or platform fees, should the transaction be quarantined or reconciled with a defined financial tolerance window?*

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
