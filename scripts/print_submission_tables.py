"""Print formatted tables for Screenshots 5, 6, and 7."""
import duckdb

con = duckdb.connect("include/warehouse_shubham.duckdb")

print("\n" + "="*80)
print("DATA QUALITY SCORECARD (dq_results)")
print("="*80)
dq_df = con.execute("""
    SELECT dimension, severity, check_name, rows_checked, rows_failed, status 
    FROM dq_results 
    ORDER BY severity DESC, dimension ASC;
""").fetchdf()
print(dq_df.to_string(index=False))

print("\n" + "="*80)
print("PIPELINE RUN AUDIT (pipeline_runs)")
print("="*80)
runs_df = con.execute("""
    SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status 
    FROM pipeline_runs 
    ORDER BY start_time DESC;
""").fetchdf()
print(runs_df.to_string(index=False))

print("\n" + "="*80)
print("PAYMENT RECONCILIATION (mart_payment_reconciliation)")
print("="*80)
recon_df = con.execute("""
    SELECT payment_mode, total_orders, reconciled_revenue, unreconciled_transactions, unreconciled_amount 
    FROM mart_payment_reconciliation 
    ORDER BY reconciled_revenue DESC;
""").fetchdf()
print(recon_df.to_string(index=False))
print("\n" + "="*80)
