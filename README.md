# NammaMart Quick Commerce Data Pipeline (FDE Module 2)

> **Enterprise-grade Apache Airflow & DuckDB Data Engineering Pipelines (ETL & ELT)**  
> **Course**: IIT Roorkee CEC & Futurense · *Data Pipeline to ML Model* (FDAIE Module 2)  
> **Upstream Repository**: [nucops/airflow-etl-elt](https://github.com/nucops/airflow-etl-elt)  
> **Reference Guide**: Based on the instructor's official *Learner Setup Guide* (`upstream/learner_setup.docx`)

---

## 🎯 Purpose of This Guide
This guide is designed for developers who need to run and develop the **NammaMart Data Pipeline**.

**Guiding Principle**: *We simplify the instructor's official setup process, without reinventing it.*  
You can run this project using the **Official Instructor Method (Astro CLI)** or the **One-Click Dev Container Method**. Both methods use the exact same underlying Astro runtime image, data models, and Airflow components.

---

## Table of Contents
1. [Core Concepts Explained](#1-core-concepts-explained-for-cs--software-engineers)
   - [What is Apache Airflow?](#what-is-apache-airflow)
   - [What is Astronomer & Astro CLI?](#what-is-astronomer--astro-cli)
   - [What is DuckDB? (Why not PostgreSQL or MySQL?)](#what-is-duckdb-why-not-postgresql-or-mysql)
   - [ETL vs. ELT: The Architectural Difference](#etl-vs-elt-the-architectural-difference)
   - [Star Schema & The Quarantine Pattern](#star-schema--the-quarantine-pattern)
2. [Instructor Setup vs. How This Repo Simplifies It](#2-instructor-setup-vs-how-this-repo-simplifies-it)
3. [System Requirements (macOS, Linux & Windows)](#3-system-requirements-macos-linux--windows)
4. [Step-by-Step Execution Guide](#4-step-by-step-execution-guide)
   - [Step 1: Prerequisites Installation](#step-1-prerequisites-installation)
   - [Step 2: Fork and Clone the Repository](#step-2-fork-and-clone-the-repository)
   - [Step 3: Start the Airflow Stack (Host Terminal)](#step-3-start-the-airflow-stack-host-terminal)
   - [Step 4: Airflow Variables & Settings](#step-4-airflow-variables--settings)
   - [Step 5: Run and Test Pipelines (Dev Container or Host)](#step-5-run-and-test-pipelines-dev-container-or-host)
   - [Step 6: Verify Outputs in DuckDB and Airflow UI](#step-6-verify-outputs-in-duckdb-and-airflow-ui)
5. [Repository & File Structure](#5-repository--file-structure)
6. [Instructor Command Reference](#6-instructor-command-reference)
7. [Common Issues and Fixes (FAQ & Troubleshooting)](#7-common-issues-and-fixes-faq--troubleshooting)

---

## 1. Core Concepts Explained


### What is Apache Airflow?
* **Backend Analogy**: Think of Airflow as **Celery / BullMQ (task queues)** + **cron** + **an observability dashboard (like Datadog / Prometheus)**, built specifically for data workflows.
* **The Problem It Solves**: In a standard backend, you might trigger a cron script `python ingest.py`. But if step 3 fails, how do you retry only step 3? How do you prevent downstream steps from running on corrupt data? How do you monitor execution history?
* **Airflow's Solution**:
  * **DAG (Directed Acyclic Graph)**: A Python script defining tasks and their execution order (e.g., `extract >> validate >> load`). "Acyclic" guarantees there are no infinite loops.
  * **Scheduler**: A background daemon that monitors DAGs, schedules runs (e.g., every day before 9:00 AM IST), and triggers tasks when upstream dependencies succeed.
  * **Webserver**: A web UI (`http://localhost:8080`) providing live visual graphs, task status, execution history, and logs.
  * **Metadata Database**: A relational database (PostgreSQL) storing DAG runs, task states, variables, and connection strings.

### What is Astronomer & Astro CLI?
* **Web Dev Analogy**: Just like **Vite** or **Next.js CLI** scaffolds and boots a full dev server with zero boilerplate, **Astronomer Astro CLI (`astro`)** is the standard tool to build, run, and test Apache Airflow locally.
* **Why It Matters**: Running raw Airflow normally requires manually configuring 5 separate Docker services (scheduler, webserver, triggerer, postgres, redis). Astro packages this into a clean runtime Docker image (`Dockerfile`) managed with single commands: `astro dev start` and `astro dev stop`.

### What is DuckDB? (Why not PostgreSQL or MySQL?)
* **CS Analogy**: **DuckDB is "SQLite for Big Data / OLAP (Online Analytical Processing)".**
* **The Difference**:
  * **PostgreSQL / MySQL (OLTP - Row-Oriented)**: Built for high-concurrency row transactions (`INSERT INTO users VALUES (...)` or `SELECT * FROM orders WHERE id = 42`). Reading millions of rows to compute an aggregate is slow because it reads entire rows from disk.
  * **DuckDB (OLAP - Columnar)**: Built for fast analytical queries and aggregations (`SELECT category, SUM(amount), AVG(delivery_minutes) FROM orders GROUP BY category`). It reads only the requested columns in vector chunks directly from disk or CSV/Parquet files at C++ speed.
  * **No Server Daemon**: Like SQLite, DuckDB is an in-process library. The entire analytical warehouse lives inside a single local file: `include/warehouse.duckdb`.

### ETL vs. ELT: The Architectural Difference

This repository implements both paradigms side-by-side:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PARADIGM 1: ETL                                   │
│  (Extract -> Validate & Quarantine in Python -> Load Clean to Star Schema) │
└─────────────────────────────────────────────────────────────────────────────┘
  CSV Files ──> [Python / Pandas] ──> Bad Records Quarantine (include/quarantine/)
                       │
                       └─── Clean Records ──> DuckDB Star Schema (fact_* / dim_*)

┌─────────────────────────────────────────────────────────────────────────────┐
│                           PARADIGM 2: ELT                                   │
│  (Extract -> Load Raw Dump into Warehouse -> Transform via In-DB SQL)       │
└─────────────────────────────────────────────────────────────────────────────┘
  CSV Files ──> DuckDB (raw_* tables) ──> Staging SQL Views ──> Mart SQL Models
```

1. **ETL (`dags/nammamart_etl.py`)**:
   * *Compute runs in Python memory before writing to storage.*
   * Data is extracted from raw CSVs, validated against quality rules in Pandas, invalid records are stripped to a quarantine directory, and clean records are transformed and loaded into a dimensional Star Schema.
2. **ELT (`dags/nammamart_elt.py`)**:
   * *Compute runs inside the warehouse engine using SQL.*
   * Raw CSV data is copied verbatim into `raw_*` database tables. Transformations and aggregations are performed downstream using pure SQL (`include/sql/`) across multiple layers: **Raw → Staging → Data Marts**.

### Star Schema & The Quarantine Pattern
* **Star Schema**: A database design optimized for analytical reporting:
  * **Fact Table (`fact_orders`)**: High-volume numerical measurements (order amount, delivery time, discounts, quantities).
  * **Dimension Tables (`dim_customers`, `dim_products`, `dim_stores`)**: Descriptive context (names, categories, pincodes) linked via foreign keys.
* **The Quarantine Pattern**: Production pipelines should **never crash** on bad input data (e.g., negative delivery times or missing prices). Instead, bad records are partitioned out and written to an isolated directory (`include/quarantine/`) with a `failure_reason` column, while clean records continue to downstream analytics.

---

## 2. Instructor Setup vs. How This Repo Simplifies It

The instructor's setup guide (`upstream/learner_setup.docx`) requires 9 manual steps. Here is how this repository simplifies each step **without reinventing the process**:

| Instructor Step (`learner_setup.docx`) | What Instructor Requires | How This Repo Simplifies It (Same Engine, Less Friction) |
|---|---|---|
| **Step 1: Install Tools** | Manually install Python, Git, Docker, Astro CLI, VS Code on host OS | **Containerized**: Works with Astro CLI on host **OR** via Dev Containers without needing host Python or Astro CLI installed. |
| **Step 2: Install VS Code Extensions** | Run 6 `code --install-extension` terminal commands | **Automated**: `.devcontainer/devcontainer.json` pre-loads Python, Pylance, Docker, Rainbow CSV, and SQLTools automatically. |
| **Step 3: Clone Repo & Submodules** | Clone repo and manage course files | **Single Git Command**: Clone with `--recurse-submodules` (course datasets in `upstream/` are linked immediately). |
| **Step 4: Install Local Python Packages** | `pip install pandas duckdb scikit-learn ...` on host | **Pre-baked**: All dependencies are pre-installed in the Astro runtime image from `requirements.txt`. |
| **Step 5: Start Airflow with Astro** | `astro dev start` | Supported via `astro dev start` **OR** standard `docker compose up -d` (uses the exact same Astro runtime image). |
| **Step 6: Add Airflow Variables** | Manually click *Admin → Variables → Add Variable* in Web UI | **Declarative IaC**: Variables, worker pools, and connections are defined in `airflow_settings.yaml` and loaded automatically. |
| **Step 7: Trigger Pipeline** | Click play button in Airflow Web UI | Supported in Web UI **AND** fast CLI unit test via `make test-etl` / `make test-elt`. |
| **Step 8 & 9: Verify Output & DQ** | Run DuckDB queries and ML scripts manually | Automated via `make test-dq` and built-in SQL models in `include/sql/`. |

---

## 3. System Requirements (macOS, Linux & Windows)

* **macOS**:
  * macOS 12 (Monterey) or later.
  * **Docker Desktop** (v4.20+) or **OrbStack** (ensure *VirtioFS* is enabled for file sharing performance).
  * **VS Code** (or Antigravity IDE) with the **Dev Containers** extension (`ms-vscode-remote.remote-containers`).
  * `git` (v2.30+).
  * *(Optional)* Astro CLI: `brew install astro`.

* **Linux (Ubuntu, Debian, Fedora, Arch)**:
  * Docker Engine (v24.0+) + Docker Compose v2 plugin (`docker-compose-plugin`).
  * Add user to the docker group: `sudo usermod -aG docker $USER`.
  * **VS Code** (or Antigravity IDE) with the **Dev Containers** extension (`ms-vscode-remote.remote-containers`).
  * `git` (v2.30+).
  * *(Optional)* Astro CLI: `curl -sSL install.astronomer.io | sudo bash`.

* **Windows (10 / 11)**:
  * Windows 10 (64-bit: Pro, Enterprise, or Home) or Windows 11 with **WSL 2 (Windows Subsystem for Linux)** installed (`wsl --install`).
  * **Docker Desktop** with the **WSL 2 backend** enabled (*Settings → General → Use the WSL 2 based engine*).
  * **VS Code** (or Antigravity IDE) with the **Dev Containers** extension (`ms-vscode-remote.remote-containers`) and **WSL** extension.
  * `git` for Windows (v2.30+).
  * *(Optional)* Astro CLI: Run in PowerShell as Administrator `winget install -e --id Astronomer.Astro`.

---

## 4. Step-by-Step Execution Guide

### Step 1: Prerequisites Installation
Follow the instructor's Step 1 recommendations:
1. **Docker Desktop**: Install from [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) and ensure it is running.
2. **VS Code**: Install from [code.visualstudio.com](https://code.visualstudio.com).
3. **Astro CLI** (Optional, if using Astro on host):
   * macOS: `brew install astro`
   * Linux: `curl -sSL install.astronomer.io | sudo bash`
   * Windows (PowerShell): `winget install -e --id Astronomer.Astro`

---

### Step 2: Fork and Clone the Repository

1. Open the repository on GitHub: [https://github.com/nucops/airflow-etl-elt](https://github.com/nucops/airflow-etl-elt).
2. Click **Fork** in the top-right corner to fork it to your own GitHub profile.
3. Clone your fork locally using `--recurse-submodules`:

```bash
# Clone with submodules so the upstream dataset is included
git clone --recurse-submodules https://github.com/YOUR_USERNAME/airflow-etl-elt.git
cd airflow-etl-elt
```

> **Note**: If you already cloned without `--recurse-submodules`, initialize them with:
> ```bash
> git submodule update --init --recursive
> ```

---

### Step 3: Start the Airflow Stack (Host Terminal)

Open a terminal on your host machine (where Docker is running).

**Option A — Official Instructor Method (Astro CLI)**:
```bash
astro dev start
```

**Option B — Standalone Docker Compose (Host-independent, No Astro CLI needed)**:
```bash
docker compose up -d
```

Both options launch the complete Airflow stack:
* **Airflow Webserver**: [http://localhost:8080](http://localhost:8080)
* **Default Login**: Username `admin` | Password `admin`
* **Metadata Database**: PostgreSQL running on port `5432`

---

### Step 4: Airflow Variables & Settings

In the instructor guide (Step 6), variables like `SLACK_WEBHOOK_URL` are added manually through the UI.

In this repository, they are codified declaratively in [`airflow_settings.yaml`](./airflow_settings.yaml):
* `NAMMAMART_DATA_DIR`: Path to daily CSV files (`include/data/nammamart`)
* `NAMMAMART_WAREHOUSE_PATH`: Local DuckDB warehouse (`include/warehouse.duckdb`)
* `NAMMAMART_QUARANTINE_PATH`: Rejection directory (`include/quarantine`)
* `SLACK_WEBHOOK_URL`: Configurable alert webhook

When you run `astro dev start`, Astro automatically imports these variables, worker pools, and connections into Airflow.

---

### Step 5: Run and Test Pipelines (Dev Container or Host)

You can work directly inside the **VS Code Dev Container** (recommended) or in your host terminal:

#### Working Inside the Dev Container
1. Open the `fdeM2` folder in VS Code.
2. Click **Reopen in Container** when prompted.
3. Open the integrated terminal and run the streamlined Makefile commands:

```bash
# Initialize directories, datasets, and metadata database
make init

# Test the ETL pipeline (Extract -> Validate -> Quarantine -> DuckDB Star Schema)
make test-etl

# Test the ELT pipeline (Raw CSV Load -> Staging SQL -> Daily Marts)
make test-elt

# Run the 6-dimension Data Quality validation suite
make test-dq

# Lint and format Python code with Ruff
make lint

# Purge cache artifacts
make clean
```

#### Triggering from the Airflow Web UI
1. Navigate to [http://localhost:8080](http://localhost:8080).
2. Locate `nammamart_etl` and `nammamart_elt`.
3. Toggle them **On** and click the **Trigger DAG (Play)** button.
4. Watch the tasks turn dark green (success) in the Graph view.

---

### Step 6: Verify Outputs in DuckDB and Airflow UI

1. **Check Clean Warehouse Data**:
   Query the DuckDB warehouse from your terminal:
   ```bash
   python -c "import duckdb; con = duckdb.connect('include/warehouse.duckdb'); print(con.execute('SHOW TABLES').fetchall())"
   ```
2. **Inspect Quarantine Records**:
   Open `include/quarantine/` in the VS Code explorer. You will see isolated CSV records with clear `failure_reason` annotations.

---

## 5. Repository & File Structure

```
fdeM2/
├── .devcontainer/
│   ├── Dockerfile                 # Astro Runtime container with DevContainer tooling
│   └── devcontainer.json          # One-click VS Code remote environment config
├── .vscode/
│   ├── launch.json                # F5 debuggers for DAGs & Data Quality checks
│   ├── settings.json              # Python interpreter, Ruff formatter & DuckDB bindings
│   └── tasks.json                 # Fast task shortcuts for starting stacks & running tests
├── upstream/                      # Git submodule tracking the official course repository
├── dags/
│   ├── nammamart_etl.py           # Production ETL DAG (Extract -> Validate -> Quarantine -> Load)
│   └── nammamart_elt.py           # Production ELT DAG (Raw -> DuckDB SQL Staging -> Marts)
├── include/
│   ├── data/
│   │   └── nammamart/             # Active CSV feeds (orders, payments, customers, products, stores)
│   ├── sql/                       # Staging and Mart SQL transformation models for DuckDB
│   ├── quarantine/                # Isolated directory storing rejected records with failure_reason
│   ├── dq_checks.py               # Reusable 6-dimension Data Quality validation framework
│   └── warehouse.duckdb           # Analytical DuckDB database file (created on pipeline run)
├── Dockerfile                     # Base Astro Runtime image used by Docker Compose & Dev Container
├── docker-compose.yml             # Standalone Airflow multi-service stack (Webserver, Scheduler, Postgres)
├── airflow_settings.yaml          # Declarative Airflow configuration (Variables, Pools, Connections)
├── airflow_settings.yaml.example  # Reference template for declarative Airflow settings
├── Makefile                       # Dev Container lifecycle commands (init, test-etl, test-elt, test-dq, lint)
├── pyproject.toml                 # Ruff linting rules and submodule exclusions
├── requirements.txt               # Pinned Python package dependencies (duckdb, pandas, ruff, etc.)
├── packages.txt                   # OS-level debian dependencies for container builds
├── SUBMISSION.md                  # Comprehensive academic submission report (Parts 0 through G)
└── README.md                      # Project documentation
```

---

## 6. Instructor Command Reference

From `upstream/learner_setup.docx`, here are the standard lifecycle commands:

| Command | What it does |
|---|---|
| `astro dev start` | Starts all Airflow Docker containers (Docker must be running) |
| `astro dev stop` | Stops Airflow and preserves persistent database volumes |
| `astro dev restart` | Rebuilds and restarts containers with new requirements |
| `astro dev logs` | Views live Airflow container logs in the terminal |
| `astro dev bash` | Opens an interactive bash shell inside the Airflow scheduler container |
| `docker ps` | Lists all running Docker containers |
| `git pull` | Fetches latest code updates from GitHub |
| `make test-etl` | Runs standalone local CLI test of `nammamart_etl` DAG |
| `make test-elt` | Runs standalone local CLI test of `nammamart_elt` DAG |
| `make test-dq` | Runs standalone test of 6-dimension Data Quality framework |

---

## 7. Common Issues and Fixes (FAQ & Troubleshooting)

The following table includes all issues listed in the instructor guide (`upstream/learner_setup.docx`) plus practical Docker solutions:

| Problem | Cause | Solution |
|---|---|---|
| `astro: command not found` | Astro CLI was installed but terminal session was not refreshed | Close and reopen your terminal after running `brew install astro` or `winget`. |
| `Docker not running error` | Docker daemon is stopped | Open Docker Desktop and wait until the status displays "Docker Desktop is running". |
| `Port 8080 already in use` | Another application or previous Airflow run is using port 8080 | Run `astro dev stop` or `docker compose down`. Alternatively, change port mapping in `docker-compose.yml` to `"8081:8080"`. |
| `Task turns red in Airflow` | Task failed during execution | Click the red task box in the Airflow UI $\rightarrow$ click **Logs** tab $\rightarrow$ read the stack trace. |
| `warehouse.duckdb not found` | The warehouse has not been generated yet | Trigger the DAG or run `make test-etl` / `make test-elt` once; it creates the database file automatically. |
| `ModuleNotFoundError locally` | Packages not installed in host virtualenv | Either work inside the **Dev Container** (pre-installed) or run `pip install -r requirements.txt` on host. |
| `astro dev start is slow` | First run image download | Normal on the first run; Docker downloads the base image layers (~3–5 minutes). Subsequent starts take seconds. |
| `IOException: Could not set lock` | DuckDB file locked by another process | DuckDB allows one writer process at a time. Close any open Python shells or IDE SQLite viewers before running a pipeline write. |
| Submodule folder `upstream/` empty | Cloned without `--recurse-submodules` | Run `git submodule update --init --recursive` to pull the course repository contents. |
