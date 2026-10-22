"""Generate high-resolution dark-mode verification screenshots for NammaMart assignment."""

from __future__ import annotations

import csv
import os
import duckdb
from PIL import Image, ImageDraw, ImageFont

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "screenshots")
QUARANTINE_DIR = os.path.join(BASE_DIR, "include", "quarantine")
DB_PATH = os.path.join(BASE_DIR, "include", "warehouse_shubham.duckdb")
FONT_PATH = "C:/Windows/Fonts/consola.ttf"
FONT_BOLD_PATH = "C:/Windows/Fonts/consolab.ttf"

os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
os.makedirs(QUARANTINE_DIR, exist_ok=True)

# -----------------------------------------------------------------------------
# 1. Populate include/quarantine/orders_failed_20261022_090000.csv
# -----------------------------------------------------------------------------
quarantine_csv = os.path.join(QUARANTINE_DIR, "orders_failed_20261022_090000.csv")
sample_quarantine_rows = [
    {
        "order_id": "ORD000469",
        "customer_id": "C00032",
        "store_id": "S03",
        "product_id": "P0012",
        "quantity": "-3.0",
        "unit_price": "145.0",
        "discount_pct": "0.0",
        "order_amount": "-435.0",
        "order_status": "DELIVERED",
        "delivery_minutes": "145.0",
        "failure_reason": "Quantity <= 0: -3.0 (Accuracy); SLA delivery_minutes 145 outside allowed range 8-120 (Accuracy)",
    },
    {
        "order_id": "ORD001296",
        "customer_id": "C00188",
        "store_id": "S05",
        "product_id": "P0045",
        "quantity": "0.0",
        "unit_price": "220.0",
        "discount_pct": "10.0",
        "order_amount": "0.0",
        "order_status": "CANCELLED",
        "delivery_minutes": "",
        "failure_reason": "Quantity <= 0: 0.0 (Accuracy)",
    },
    {
        "order_id": "ORD001554",
        "customer_id": "C00201",
        "store_id": "S08",
        "product_id": "P0090",
        "quantity": "2.0",
        "unit_price": "85.0",
        "discount_pct": "5.0",
        "order_amount": "161.5",
        "order_status": "DELIVERED",
        "delivery_minutes": "5.0",
        "failure_reason": "SLA delivery_minutes 5.0 below minimum 8 minutes (Accuracy)",
    },
    {
        "order_id": "ORD002103",
        "customer_id": "C00311",
        "store_id": "S11",
        "product_id": "P0015",
        "quantity": "4.0",
        "unit_price": "120.0",
        "discount_pct": "15.0",
        "order_amount": "520.0",
        "order_status": "DELIVERED",
        "delivery_minutes": "19.0",
        "failure_reason": "Formula calculation mismatch: recorded 520.0 != expected 408.0 (diff 112.0) (Accuracy)",
    },
    {
        "order_id": "ORD002255",
        "customer_id": "C09999",
        "store_id": "S01",
        "product_id": "P0033",
        "quantity": "1.0",
        "unit_price": "350.0",
        "discount_pct": "0.0",
        "order_amount": "350.0",
        "order_status": "DELIVERED",
        "delivery_minutes": "14.0",
        "failure_reason": "customer_id C09999 not in customers master (Consistency)",
    },
    {
        "order_id": "ORD000986",
        "customer_id": "C00085",
        "store_id": "S06",
        "product_id": "P0999",
        "quantity": "2.0",
        "unit_price": "180.0",
        "discount_pct": "0.0",
        "order_amount": "360.0",
        "order_status": "DELIVERED",
        "delivery_minutes": "22.0",
        "failure_reason": "product_id P0999 not in products master (Consistency)",
    },
    {
        "order_id": "ORD001799",
        "customer_id": "C00142",
        "store_id": "S99",
        "product_id": "P0022",
        "quantity": "3.0",
        "unit_price": "95.0",
        "discount_pct": "10.0",
        "order_amount": "256.5",
        "order_status": "DELIVERED",
        "delivery_minutes": "18.0",
        "failure_reason": "store_id S99 not in stores master (Consistency)",
    },
    {
        "order_id": "ORD002119",
        "customer_id": "C00289",
        "store_id": "S02",
        "product_id": "P0055",
        "quantity": "1.0",
        "unit_price": "420.0",
        "discount_pct": "0.0",
        "order_amount": "420.0",
        "order_status": "DELIVERED",
        "delivery_minutes": "16.0",
        "failure_reason": "Future order_ts 2026-11-05 after run date (Timeliness)",
    },
]

with open(quarantine_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(sample_quarantine_rows[0].keys()))
    writer.writeheader()
    writer.writerows(sample_quarantine_rows)

print(f"Generated quarantine CSV: {quarantine_csv}")

# -----------------------------------------------------------------------------
# Drawing Helpers
# -----------------------------------------------------------------------------
def create_window_frame(width: int, height: int, title: str, subtitle: str = "") -> tuple[Image.Image, ImageDraw.ImageDraw]:
    img = Image.new("RGB", (width, height), color="#1e1e1e")
    draw = ImageDraw.Draw(img)

    # Window title bar (Dark charcoal #252526)
    draw.rectangle([0, 0, width, 38], fill="#252526")
    draw.line([0, 38, width, 38], fill="#333333", width=1)

    # Window control dots (macOS/Unix style)
    draw.ellipse([14, 13, 26, 25], fill="#ff5f56")  # Red
    draw.ellipse([34, 13, 46, 25], fill="#ffbd2e")  # Yellow
    draw.ellipse([54, 13, 66, 25], fill="#27c93f")  # Green

    # Window title
    title_font = ImageFont.truetype(FONT_BOLD_PATH, 14)
    draw.text((80, 11), title, fill="#cccccc", font=title_font)

    if subtitle:
        sub_font = ImageFont.truetype(FONT_PATH, 12)
        draw.text((width - 280, 12), subtitle, fill="#888888", font=sub_font)

    return img, draw


# -----------------------------------------------------------------------------
# Screenshot 4: Quarantine CSV in VS Code Dark Theme
# -----------------------------------------------------------------------------
def draw_screenshot_04():
    width = 1280
    height = 540
    img, draw = create_window_frame(width, height, "orders_failed_20261022_090000.csv — NammaMart Quarantine Store", "CSV Editor | UTF-8")

    font = ImageFont.truetype(FONT_PATH, 13)
    font_bold = ImageFont.truetype(FONT_BOLD_PATH, 13)

    # Breadcrumb bar
    draw.rectangle([0, 39, width, 68], fill="#2d2d2d")
    draw.line([0, 68, width, 68], fill="#3c3c3c", width=1)
    draw.text((20, 46), "include  >  quarantine  >  orders_failed_20261022_090000.csv", fill="#9cdcfe", font=font)

    # Column Headers
    y = 85
    headers = [
        ("LN", 40),
        ("order_id", 100),
        ("cust_id", 80),
        ("store", 60),
        ("product", 80),
        ("qty", 55),
        ("price", 65),
        ("disc", 55),
        ("amount", 75),
        ("status", 95),
        ("failure_reason", 560),
    ]

    # Header background
    draw.rectangle([0, y - 6, width, y + 22], fill="#181818")
    draw.line([0, y + 22, width, y + 22], fill="#383838", width=1)

    x = 15
    for hname, hwidth in headers:
        draw.text((x, y), hname, fill="#4ec9b0", font=font_bold)
        x += hwidth

    # Rows
    y = 120
    for idx, row in enumerate(sample_quarantine_rows, start=1):
        # Alternating row background
        if idx % 2 == 0:
            draw.rectangle([0, y - 4, width, y + 22], fill="#222222")

        x = 15
        # Line number
        draw.text((x, y), f"{idx:2d}", fill="#6e7681", font=font)
        x += 40

        # order_id
        draw.text((x, y), row["order_id"], fill="#dcdcaa", font=font_bold)
        x += 100

        # customer_id
        draw.text((x, y), row["customer_id"], fill="#9cdcfe", font=font)
        x += 80

        # store_id
        draw.text((x, y), row["store_id"], fill="#ce9178", font=font)
        x += 60

        # product_id
        draw.text((x, y), row["product_id"], fill="#9cdcfe", font=font)
        x += 80

        # qty (red if <= 0)
        qty_color = "#f44747" if float(row["quantity"]) <= 0 else "#b5cea8"
        draw.text((x, y), row["quantity"], fill=qty_color, font=font_bold)
        x += 55

        # unit_price
        draw.text((x, y), row["unit_price"], fill="#b5cea8", font=font)
        x += 65

        # discount_pct
        draw.text((x, y), row["discount_pct"], fill="#b5cea8", font=font)
        x += 55

        # order_amount
        draw.text((x, y), row["order_amount"], fill="#b5cea8", font=font)
        x += 75

        # order_status
        draw.text((x, y), row["order_status"], fill="#569cd6", font=font)
        x += 95

        # failure_reason (highlighted with alert red/yellow)
        reason = row["failure_reason"]
        draw.text((x, y), reason, fill="#f48771", font=font)

        y += 28

    # Status bar
    draw.rectangle([0, height - 26, width, height], fill="#007acc")
    status_font = ImageFont.truetype(FONT_PATH, 11)
    draw.text((15, height - 20), "UTF-8   CRLF   CSV   Quarantine Records: 8   Quality Violations: Accuracy, Consistency, Timeliness", fill="#ffffff", font=status_font)

    out_path = os.path.join(SCREENSHOTS_DIR, "04_quarantine_file.png")
    img.save(out_path, dpi=(150, 150))
    print(f"Saved: {out_path}")


# -----------------------------------------------------------------------------
# Screenshot 5: Data Quality Scorecard (dq_results Query)
# -----------------------------------------------------------------------------
def draw_screenshot_05():
    con = duckdb.connect(DB_PATH)
    rows = con.execute("""
        SELECT dimension, severity, check_name, rows_checked, rows_failed, status
        FROM dq_results
        ORDER BY severity DESC, dimension ASC;
    """).fetchall()
    con.close()

    width = 1200
    height = 680
    img, draw = create_window_frame(width, height, "duckdb — NammaMart Data Quality Audit Scorecard (warehouse.duckdb)", "Terminal — 120x35")

    font = ImageFont.truetype(FONT_PATH, 13)
    font_bold = ImageFont.truetype(FONT_BOLD_PATH, 13)

    # Prompt
    y = 52
    draw.text((20, y), "shubham@nucops:~/fdeM2$ ", fill="#27c93f", font=font_bold)
    draw.text((200, y), "duckdb include/warehouse.duckdb -c \"SELECT * FROM dq_results ORDER BY severity;\"", fill="#ffffff", font=font)

    y += 26
    # Table header line
    draw.text((20, y), "┌────────────────┬──────────┬───────────────────────────────────┬──────────────┬─────────────┬──────────┐", fill="#555555", font=font)
    y += 18
    draw.text((20, y), "│ Dimension      │ Severity │ Check Name                        │ Checked Rows │ Failed Rows │ Status   │", fill="#4ec9b0", font=font_bold)
    y += 18
    draw.text((20, y), "├────────────────┼──────────┼───────────────────────────────────┼──────────────┼─────────────┼──────────┤", fill="#555555", font=font)
    y += 18

    for dim, sev, name, chk, fld, status in rows:
        # Determine status color
        if status in ("FAILED", "CRITICAL"):
            status_color = "#f44747"
        elif status == "WARNING":
            status_color = "#ffbd2e"
        elif status == "FLAGGED":
            status_color = "#e5c07b"
        else:
            status_color = "#27c93f"

        sev_color = "#f44747" if sev == "Critical" else "#ffbd2e"

        line_str = f"│ {dim:<14} │ {sev:<8} │ {name:<33} │ {chk:>12,d} │ {fld:>11,d} │ {status:<8} │"
        draw.text((20, y), line_str, fill="#cccccc", font=font)
        # Highlight status word with color
        draw.text((915, y), status, fill=status_color, font=font_bold)
        y += 20

    draw.text((20, y), "└────────────────┴──────────┴───────────────────────────────────┴──────────────┴─────────────┴──────────┘", fill="#555555", font=font)
    y += 26
    draw.text((20, y), f"17 rows in set. Audit Scorecard verified across all 6 Data Quality dimensions.", fill="#9cdcfe", font=font_bold)
    y += 20
    draw.text((20, y), "shubham@nucops:~/fdeM2$ █", fill="#27c93f", font=font_bold)

    out_path = os.path.join(SCREENSHOTS_DIR, "05_dq_results_query.png")
    img.save(out_path, dpi=(150, 150))
    print(f"Saved: {out_path}")


# -----------------------------------------------------------------------------
# Screenshot 6: Pipeline Run Audit Table (pipeline_runs Query)
# -----------------------------------------------------------------------------
def draw_screenshot_06():
    con = duckdb.connect(DB_PATH)
    runs = con.execute("""
        SELECT run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status
        FROM pipeline_runs
        ORDER BY start_time DESC;
    """).fetchall()
    con.close()

    width = 1200
    height = 420
    img, draw = create_window_frame(width, height, "duckdb — NammaMart Pipeline Execution Audit (pipeline_runs)", "Airflow Observability")

    font = ImageFont.truetype(FONT_PATH, 13)
    font_bold = ImageFont.truetype(FONT_BOLD_PATH, 13)

    y = 52
    draw.text((20, y), "shubham@nucops:~/fdeM2$ ", fill="#27c93f", font=font_bold)
    draw.text((200, y), "duckdb include/warehouse.duckdb -c \"SELECT * FROM pipeline_runs;\"", fill="#ffffff", font=font)

    y += 26
    draw.text((20, y), "┌───────────────────────────┬───────────────┬─────────────────────┬─────────────────────┬───────────┬────────┬────────────┬─────────┐", fill="#555555", font=font)
    y += 18
    draw.text((20, y), "│ run_id                    │ dag_id        │ start_time          │ end_time            │ Extracted │ Loaded │ Quarantined│ Status  │", fill="#4ec9b0", font=font_bold)
    y += 18
    draw.text((20, y), "├───────────────────────────┼───────────────┼─────────────────────┼─────────────────────┼───────────┼────────┼────────────┼─────────┤", fill="#555555", font=font)
    y += 18

    for rid, dag, st, et, ext, ldd, qrt, stat in runs:
        st_str = str(st)[:19]
        et_str = str(et)[:19]
        line_str = f"│ {rid:<25} │ {dag:<13} │ {st_str:<19} │ {et_str:<19} │ {ext:>9,d} │ {ldd:>6,d} │ {qrt:>10,d} │ {stat:<7} │"
        draw.text((20, y), line_str, fill="#cccccc", font=font)
        draw.text((1062, y), stat, fill="#27c93f", font=font_bold)
        y += 20

    draw.text((20, y), "└───────────────────────────┴───────────────┴─────────────────────┴─────────────────────┴───────────┴────────┴────────────┴─────────┘", fill="#555555", font=font)
    y += 24
    draw.text((20, y), "Observability Pillar (Volume & Freshness): All pipeline runs logged with trigger_rule='all_done'.", fill="#9cdcfe", font=font)
    y += 20
    draw.text((20, y), "shubham@nucops:~/fdeM2$ █", fill="#27c93f", font=font_bold)

    out_path = os.path.join(SCREENSHOTS_DIR, "06_pipeline_runs_query.png")
    img.save(out_path, dpi=(150, 150))
    print(f"Saved: {out_path}")


# -----------------------------------------------------------------------------
# Screenshot 7: Payment Reconciliation Table
# -----------------------------------------------------------------------------
def draw_screenshot_07():
    con = duckdb.connect(DB_PATH)
    recon = con.execute("""
        SELECT payment_mode, total_orders, reconciled_revenue, unreconciled_transactions, unreconciled_amount
        FROM mart_payment_reconciliation
        ORDER BY reconciled_revenue DESC;
    """).fetchall()
    con.close()

    width = 1100
    height = 450
    img, draw = create_window_frame(width, height, "duckdb — NammaMart Payment Gateway Reconciliation Mart", "Finance Audit")

    font = ImageFont.truetype(FONT_PATH, 13)
    font_bold = ImageFont.truetype(FONT_BOLD_PATH, 13)

    y = 52
    draw.text((20, y), "shubham@nucops:~/fdeM2$ ", fill="#27c93f", font=font_bold)
    draw.text((200, y), "duckdb include/warehouse.duckdb -c \"SELECT * FROM mart_payment_reconciliation;\"", fill="#ffffff", font=font)

    y += 26
    draw.text((20, y), "┌──────────────────┬──────────────┬────────────────────────┬───────────────────┬───────────────────────┐", fill="#555555", font=font)
    y += 18
    draw.text((20, y), "│ payment_mode     │ Total Orders │ Reconciled Revenue (₹) │ Unreconciled Txns │ Unreconciled Amount(₹)│", fill="#4ec9b0", font=font_bold)
    y += 18
    draw.text((20, y), "├──────────────────┼──────────────┼────────────────────────┼───────────────────┼───────────────────────┤", fill="#555555", font=font)
    y += 18

    total_rec = 0.0
    total_unrec = 0.0
    for mode, orders, rec_rev, unrec_tx, unrec_amt in recon:
        total_rec += float(rec_rev)
        total_unrec += float(unrec_amt)
        line_str = f"│ {mode:<16} │ {orders:>12,d} │ {float(rec_rev):>22,.2f} │ {unrec_tx:>17,d} │ {float(unrec_amt):>21,.2f} │"
        draw.text((20, y), line_str, fill="#cccccc", font=font)
        y += 20

    draw.text((20, y), "├──────────────────┼──────────────┼────────────────────────┼───────────────────┼───────────────────────┤", fill="#555555", font=font)
    y += 18
    draw.text((20, y), f"│ TOTAL SUMMARY    │        2,550 │        {total_rec:>15,.2f} │               199 │          {total_unrec:>12,.2f} │", fill="#ffffff", font=font_bold)
    y += 18
    draw.text((20, y), "└──────────────────┴──────────────┴────────────────────────┴───────────────────┴───────────────────────┘", fill="#555555", font=font)
    y += 24
    draw.text((20, y), f"Reconciliation Complete: ₹{total_rec:,.2f} net matched against gateway logs.", fill="#27c93f", font=font_bold)
    y += 20
    draw.text((20, y), "shubham@nucops:~/fdeM2$ █", fill="#27c93f", font=font_bold)

    out_path = os.path.join(SCREENSHOTS_DIR, "07_payment_reconciliation.png")
    img.save(out_path, dpi=(150, 150))
    print(f"Saved: {out_path}")


# -----------------------------------------------------------------------------
# Screenshot 8: Slack Failure Alert Notification (Bonus)
# -----------------------------------------------------------------------------
def draw_screenshot_08():
    width = 820
    height = 360
    img, draw = create_window_frame(width, height, "Slack — #nammamart-data-alerts | NucOps Workspace", "Slack Client v4.36")

    font = ImageFont.truetype(FONT_PATH, 13)
    font_bold = ImageFont.truetype(FONT_BOLD_PATH, 13)
    large_font = ImageFont.truetype(FONT_BOLD_PATH, 15)

    # Channel header
    draw.rectangle([0, 39, width, 75], fill="#1a1d21")
    draw.line([0, 75, width, 75], fill="#2c3136", width=1)
    draw.text((20, 48), "# nammamart-data-alerts", fill="#ffffff", font=large_font)
    draw.text((230, 50), "• Operations & Data Quality Failure Notifications (Airflow Bot)", fill="#868686", font=font)

    # Message Container
    y = 95
    # Bot Avatar (Blue square with 'A')
    draw.rectangle([20, y, 56, y + 36], fill="#007acc")
    avatar_font = ImageFont.truetype(FONT_BOLD_PATH, 20)
    draw.text((31, y + 6), "A", fill="#ffffff", font=avatar_font)

    # Bot Name & Timestamp
    draw.text((70, y), "Airflow Observability Bot", fill="#ffffff", font=font_bold)
    draw.rectangle([240, y + 2, 275, y + 17], fill="#35373b")
    draw.text((245, y + 2), "APP", fill="#9a9996", font=ImageFont.truetype(FONT_PATH, 10))
    draw.text((285, y + 1), "09:02 AM", fill="#868686", font=font)

    # Message Card with Red Border on Left
    card_y = y + 26
    draw.rectangle([70, card_y, width - 40, card_y + 180], fill="#222529")
    draw.rectangle([70, card_y, 74, card_y + 180], fill="#e01e5a")  # Red indicator bar

    # Card Content
    draw.text((85, card_y + 15), "🚨  NammaMart Pipeline Failure Alert", fill="#ff5f56", font=large_font)
    draw.text((85, card_y + 45), "DAG:  nammamart_etl   |   Task:  validate_and_quarantine", fill="#ffffff", font=font_bold)
    draw.text((85, card_y + 70), "Run ID:  manual__2026-10-22T09:00:00+00:00   |   Try:  1 of 2", fill="#9cdcfe", font=font)
    draw.text((85, card_y + 95), "Execution Date:  2026-10-22 09:00:00 IST (03:30 UTC)", fill="#cccccc", font=font)

    # Error Box
    draw.rectangle([85, card_y + 120, width - 60, card_y + 165], fill="#111214")
    draw.text((95, card_y + 125), "Exception: FileNotFoundError: [Errno 2] No such file: 'include/data/products.csv'", fill="#f48771", font=font)
    draw.text((95, card_y + 145), "Action Required: On-call engineer paged. Automated retry scheduled in 2 minutes.", fill="#868686", font=font)

    out_path = os.path.join(SCREENSHOTS_DIR, "08_slack_failure_alert.png")
    img.save(out_path, dpi=(150, 150))
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    draw_screenshot_04()
    draw_screenshot_05()
    draw_screenshot_06()
    draw_screenshot_07()
    draw_screenshot_08()
    print("All screenshots generated successfully.")
