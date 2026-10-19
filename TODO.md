# NammaMart ETL & ELT Pipeline Solution — Master Roadmap & TODO

> **Repository**: [nucops/airflow-etl-elt](https://github.com/nucops/airflow-etl-elt) (`fdeM2`)  
> **Course**: IIT Roorkee CEC & Futurense · *Data Pipeline to ML Model* (FDAIE Module 2)  
> **Assignment Reference**: `assignment/ASSIGNMENT.md`  
> **Target Branch**: `solution`  
> **Target Architecture**: **Pure Kubernetes Workload** deployable via Kustomize & Flux CD following [`nucops/public-workloads`](https://github.com/nucops/public-workloads) (Astro CLI and Docker Compose deprecated)

---

## 🏗️ Architectural Vision: Pure Cloud-Native Kubernetes Workload

Per architectural directive, this solution runs **exclusively on Kubernetes** rather than using local developer tools like `astro dev` or `docker compose`. The solution mirrors the GitOps structure of [`nucops/public-workloads`](https://github.com/nucops/public-workloads):

```text
fdeM2/
├── apps/
│   └── nammamart/
│       ├── namespace.yaml           # PSS baseline/restricted labels
│       ├── configmap.yaml           # Airflow & DuckDB environment configuration
│       ├── pvc.yaml                 # Persistent storage for warehouse & quarantine files
│       ├── postgres.yaml            # Metadata store (Deployment + Service)
│       ├── webserver.yaml           # Airflow Web/API UI (Deployment + Probes + Non-Root)
│       ├── scheduler.yaml           # Airflow Scheduler (Deployment + DAG execution)
│       ├── service.yaml             # ClusterIP service routing to Web UI (port 8080)
│       ├── ingress.yaml             # Traefik Ingress with ${CLUSTER_DOMAIN} & Homepage metadata
│       └── kustomization.yaml       # App-tier Kustomization with standard metadata labels
├── kustomization.yaml               # Root aggregation Kustomization
├── .github/
│   └── workflows/
│       └── validate.yaml            # CI schema validation via kubeconform & secret rejection
├── dags/
│   ├── nammamart_etl.py             # ETL Pipeline DAG (SLA: 09:00 IST)
│   └── nammamart_elt.py             # ELT Pipeline DAG (SLA: 09:00 IST)
├── include/
│   ├── data/nammamart/              # Source CSV datasets
│   ├── dq_checks.py                 # 6-dimension data quality assertions
│   └── sql/                         # Staging & Mart SQL models
├── Dockerfile                       # Hardened production container image
└── SUBMISSION.md                    # Official assignment deliverables & audit evidence
```

### Key Kubernetes Specifications (`nucops/public-workloads` Standards)
1. **Namespace Isolation & Pod Security Standards (PSS)**:
   - Dedicated namespace: `nammamart` (or `data-pipeline`).
   - Labels: `pod-security.kubernetes.io/enforce: baseline`, `pod-security.kubernetes.io/audit: restricted`, `pod-security.kubernetes.io/warn: restricted`.
2. **Hardened Security Context**:
   - `runAsNonRoot: true` (UID 50000 / `astro` user).
   - `allowPrivilegeEscalation: false`.
   - `capabilities: { drop: ["ALL"] }`.
   - `seccompProfile: { type: "RuntimeDefault" }`.
   - `automountServiceAccountToken: false` on pods that do not require k8s API access.
3. **Resource Sizing & Health Probes**:
   - Bounded CPU/Memory requests and limits on every container.
   - Declarative `startupProbe`, `livenessProbe`, and `readinessProbe` on HTTP services.
4. **Flux CD Post-Build Substitution**:
   - Host routing parameterized via `${CLUSTER_DOMAIN}` (e.g., `nammamart.${CLUSTER_DOMAIN}`).
5. **CI Manifest Validation**:
   - Automated workflow running `kubectl kustomize .`, rejecting any Secret or SealedSecret, and strictly validating schemas via `kubeconform`.

---

## 📋 Comprehensive Implementation Checklist

### Phase 1: Pure Kubernetes Kustomize Infrastructure (`apps/fde/`)
- [x] **Namespace Manifest (`apps/fde/namespace.yaml`)**:
  - Namespace `fde` with PSS baseline and audit/warn labels (cluster annotations decoupled).
- [x] **ConfigMap Manifest (`apps/fde/airflow/values-configmap.yaml`)**:
  - Helm chart values with non-sensitive Airflow configs, generic admin user, and cluster telemetry decoupled.
- [x] **Metadata Database Cluster (`apps/fde/airflow/postgres.yaml`)**:
  - CloudNativePG PostgreSQL 16 cluster with parameterized storage class `${DB_SC}`.
- [x] **HelmRelease & Repository (`apps/fde/airflow/release.yaml`, `repository.yaml`)**:
  - Official Apache Airflow Helm repository and HelmRelease 1.22.0.
- [x] **Ingress Manifest (`apps/fde/airflow/ingress.yaml`)**:
  - Traefik IngressRoute with `airflow.${CLUSTER_DOMAIN}` and Homepage integration (`gethomepage.dev/*`).
- [x] **Kustomization Files**:
  - `apps/fde/airflow/kustomization.yaml`: aggregates release, repository, postgres, values-configmap, ingress (no secret resources).
  - `apps/fde/kustomization.yaml`: aggregates namespace and airflow sub-tier.
  - Root `kustomization.yaml`: aggregates `./apps/fde`.
- [x] **Reference Example Template (`apps/fde/example.yaml`)**:
  - Consolidated, fully commented reference template demonstrating private database secrets and Flux GitRepository/Kustomization with consumer patches.
- [x] **CI Validation Workflow (`.github/workflows/validate.yaml`)**:
  - Automated GitHub Actions workflow rendering `kubectl kustomize .`, strictly rejecting Secrets/SealedSecrets, and validating schemas via `kubeconform`.

---

### Phase 2: Assignment Data Profiling & Discovery (Part 0)
- [ ] **Stage Dataset in Container & PVC**:
  - Stage all 6 CSV files in `include/data/nammamart/`: `orders.csv`, `payments.csv`, `customers.csv`, `products.csv`, `stores.csv`, `orders_2026-10-01_increment.csv`.
- [ ] **Data Profiling Execution**:
  - Script/Task profiling row counts, null distributions, key uniqueness, and numeric distributions.
  - Check referential integrity across orders → customers, orders → products, orders → stores, payments → orders.
  - Validate formula consistency: `order_amount == quantity * unit_price * (1 - discount_pct / 100)`.
- [ ] **Populate Data Issues Log (`SUBMISSION.md`)**:
  - Document issues across all 5 master files + increment file with dimension and severity.
- [ ] **Record 3 Clarifying Business Questions**.

---

### Phase 3: Production ETL Pipeline in Kubernetes (Part A)
- [ ] **DAG Architecture (`dags/nammamart_etl.py`)**:
  - Scheduled at `0 9 * * 1-6` (09:00 IST Monday-Saturday).
  - Explicit timezone: `pendulum.timezone("Asia/Kolkata")`, `catchup=False`.
- [ ] **Task 1: Parallel Extraction**:
  - Extract tasks for the 5 entities reading from shared storage.
- [ ] **Task 2: Validation & Quarantine**:
  - Python quality rule engine evaluating Critical checks.
  - Corrupt rows written to `include/quarantine/<run_id>/<file>_failed.csv` with `failure_reason`.
- [ ] **Task 3: Transformation & PII Protection**:
  - Casing normalization and deduplication.
  - PII masking conforming to Indian DPDP Act 2023 (`pr***@example.com`, `98******21`).
  - Join orders with dimensions and compute derived fields (`net_revenue`, `gross_margin`, `within_15_min`).
- [ ] **Task 4: DuckDB Star Schema Loading**:
  - Idempotent table management in `include/warehouse.duckdb`:
    - Fact: `etl_fact_orders`
    - Dimensions: `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`
- [ ] **Task 5: Payment Gateway Reconciliation**:
  - Reconcile clean orders against `payments.csv`, creating `etl_payment_reconciliation`.
- [ ] **Task 6: Daily KPI Reporting**:
  - Generate `etl_daily_zone_revenue` and `etl_category_margin`.

---

### Phase 4: Production ELT Pipeline in Kubernetes (Part B)
- [ ] **DAG Architecture (`dags/nammamart_elt.py`)**:
  - Scheduled at `0 9 * * 1-6` (09:00 IST Monday-Saturday).
- [ ] **Layer 1: Raw Ingestion**:
  - Unmodified ingestion into DuckDB tables: `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores`.
  - Add metadata columns: `_loaded_at`, `_source_file`, `_run_id`.
- [ ] **Layer 2: Staging SQL Models (`include/sql/staging/`)**:
  - SQL-driven type casting, deduplication, PII masking, and DQ flagging (`dq_status`, `failure_reason`).
  - Tables: `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores`.
- [ ] **Incremental Upsert / MERGE**:
  - Process `orders_2026-10-01_increment.csv`.
  - Raw: Append rows.
  - Staging: SQL `MERGE` on `order_id` updating returned statuses and appending new orders without duplicates.
- [ ] **Layer 3: Analytical Data Marts (`include/sql/marts/`)**:
  - `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers`.
- [ ] **ETL vs. ELT September Parity Verification**:
  - Confirm Mart totals match ETL Fact numbers exactly.

---

### Phase 5: Data Quality Dimensions & Governance (Parts D & E)
- [ ] **All 6 Data Quality Dimensions**:
  - Completeness, Uniqueness, Validity, Accuracy, Consistency, Timeliness.
  - Implemented in Python (ETL) and SQL (ELT).
- [ ] **Quality Scorecard Table (`dq_results`)**:
  - Persistent tracking: `run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`.
- [ ] **Governance Answers in `SUBMISSION.md`**:
  - Ownership, Data Dictionary, Lineage demonstration, DPDP Act 2023 compliance, RBAC, Retention, Secrets management.

---

### Phase 6: Observability Suite & Client Recommendation (Parts F & G)
- [ ] **Run Audit Table (`pipeline_runs`)**:
  - Final task with `trigger_rule="all_done"` recording run metrics and duration.
- [ ] **10 Observability Experiments**:
  - Record results for Happy path, Idempotency, Incremental load, Missing file, Bad data, Schema drift, Volume anomaly, Distribution drift, Freshness, and Recovery.
- [ ] **5 Observability Pillars Mapping**:
  - Analyze Freshness, Volume, Schema, Distribution, Lineage.
- [ ] **Executive Recommendation Memo**:
  - Address CTO Sameer comparing ETL vs. ELT with trade-offs, scalability, and 3 key risks.
- [ ] **SQL Business Solutions**:
  - Deliver executable queries and outputs for all 6 business questions.

---

## 🚀 Deployment & Local Validation Commands

### Render and Validate Kubernetes Manifests
```bash
# Render Kustomization locally
kubectl kustomize .

# Validate schema compliance without applying
kubectl kustomize . | kubeconform -strict -summary
```

### Deploy to Kubernetes Cluster via Flux
```yaml
apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata:
  name: nammamart-workload
  namespace: flux-system
spec:
  interval: 5m
  url: https://github.com/nucops/airflow-etl-elt
  ref:
    branch: solution
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: nammamart-workload
  namespace: flux-system
spec:
  interval: 10m
  path: ./apps/nammamart
  prune: true
  wait: true
  timeout: 5m
  sourceRef:
    kind: GitRepository
    name: nammamart-workload
  postBuild:
    substitute:
      CLUSTER_DOMAIN: example.test
```
