# Assignment Screenshots Directory

Save all your assignment verification screenshots in this directory with the following exact filenames so they automatically link into `SUBMISSION.md`:

| # | Filename | Description | Where to capture |
|---|---|---|---|
| 1 | `01_dag_graph_etl.png` | `nammamart_etl` DAG Graph View (All Green) | Airflow UI -> DAGs -> `nammamart_etl` -> Graph View |
| 2 | `02_dag_graph_elt.png` | `nammamart_elt` DAG Graph View (All Green) | Airflow UI -> DAGs -> `nammamart_elt` -> Graph View |
| 3 | `03_schedule_9am_ist.png` | Schedule showing `09:00 IST` & Next Run | Airflow UI -> DAGs list or Grid View header |
| 4 | `04_quarantine_file.png` | Quarantine CSV file showing `failure_reason` | VS Code / Excel opening `include/quarantine/...csv` |
| 5 | `05_dq_results_query.png` | `dq_results` table query results | Terminal (DuckDB CLI) or VS Code SQLTools |
| 6 | `06_pipeline_runs_query.png` | `pipeline_runs` audit table query results | Terminal (DuckDB CLI) or VS Code SQLTools |
| 7 | `07_payment_reconciliation.png` | Payment reconciliation query results | Terminal (DuckDB CLI) or VS Code SQLTools |
| 8 | `08_slack_failure_alert.png` *(Bonus)* | Slack channel failure alert notification | Slack Desktop / Web client |
| 9 | `09_dq_trend_chart.png` *(Bonus)* | Data quality longitudinal trend chart | Python / Matplotlib / BI Dashboard |

---
*Tip*: Windows shortcut to take clean snips: `Win + Shift + S`.
