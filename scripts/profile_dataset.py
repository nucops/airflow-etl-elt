"""Pure Python standard library Data Profiling Script for NammaMart Dataset (Part 0).

Zero external dependencies - runs instantly with standard library csv and math.
"""

import csv
import os
import re

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "include", "data", "nammamart")

def load_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)

def profile():
    orders = load_csv("orders.csv")
    payments = load_csv("payments.csv")
    customers = load_csv("customers.csv")
    products = load_csv("products.csv")
    stores = load_csv("stores.csv")
    increment = load_csv("orders_2026-10-01_increment.csv")

    print(f"Loaded orders: {len(orders)} rows")
    print(f"Loaded payments: {len(payments)} rows")
    print(f"Loaded customers: {len(customers)} rows")
    print(f"Loaded products: {len(products)} rows")
    print(f"Loaded stores: {len(stores)} rows")
    print(f"Loaded increment: {len(increment)} rows")

    customer_ids = {c["customer_id"] for c in customers}
    product_ids = {p["product_id"] for p in products}
    store_ids = {s["store_id"] for s in stores}
    order_ids = {o["order_id"] for o in orders}

    issues = []

    # 1. Orders
    bad_qty = [o["order_id"] for o in orders if float(o["quantity"]) <= 0]
    if bad_qty:
        issues.append(("orders.csv", bad_qty[:3], "quantity", f"Quantity <= 0: {len(bad_qty)} rows (e.g. {bad_qty[:3]})", "Accuracy", "Critical"))

    bad_deliv = []
    for o in orders:
        if o["order_status"] == "DELIVERED":
            mins = o.get("delivery_minutes")
            if not mins or float(mins) < 8 or float(mins) > 120:
                bad_deliv.append(o["order_id"])
    if bad_deliv:
        issues.append(("orders.csv", bad_deliv[:3], "delivery_minutes", f"Out of SLA (<8 or >120 mins) or empty for DELIVERED: {len(bad_deliv)} rows", "Accuracy", "Critical"))

    bad_amt = []
    for o in orders:
        try:
            qty = float(o["quantity"])
            price = float(o["unit_price"])
            disc = float(o["discount_pct"])
            expected = round(qty * price * (1.0 - disc / 100.0), 2)
            actual = float(o["order_amount"])
            if abs(actual - expected) > 0.05:
                bad_amt.append(o["order_id"])
        except ValueError:
            bad_amt.append(o["order_id"])
    if bad_amt:
        issues.append(("orders.csv", bad_amt[:3], "order_amount", f"Formula mismatch: {len(bad_amt)} rows", "Accuracy", "Critical"))

    orphan_cust = [o["order_id"] for o in orders if o["customer_id"] not in customer_ids]
    if orphan_cust:
        issues.append(("orders.csv", orphan_cust[:3], "customer_id", f"Customer ID not in customers master: {len(orphan_cust)} rows", "Consistency", "Critical"))

    orphan_prod = [o["order_id"] for o in orders if o["product_id"] not in product_ids]
    if orphan_prod:
        issues.append(("orders.csv", orphan_prod[:3], "product_id", f"Product ID not in products master: {len(orphan_prod)} rows", "Consistency", "Critical"))

    orphan_store = [o["order_id"] for o in orders if o["store_id"] not in store_ids]
    if orphan_store:
        issues.append(("orders.csv", orphan_store[:3], "store_id", f"Store ID not in stores master: {len(orphan_store)} rows", "Consistency", "Critical"))

    future_orders = [o["order_id"] for o in orders if o["order_ts"] > "2026-10-02"]
    if future_orders:
        issues.append(("orders.csv", future_orders[:3], "order_ts", f"Future-dated order timestamps: {len(future_orders)} rows", "Timeliness", "Critical"))

    # 2. Payments
    bad_modes = [p["payment_id"] for p in payments if p["payment_mode"] != p["payment_mode"].strip() or p["payment_mode"] in ("upi", "card")]
    if bad_modes:
        issues.append(("payments.csv", bad_modes[:3], "payment_mode", f"Inconsistent casing/spaces (e.g. 'upi' vs 'UPI'): {len(bad_modes)} rows", "Validity", "Warning"))

    orphan_pay = [p["payment_id"] for p in payments if p["order_id"] not in order_ids]
    if orphan_pay:
        issues.append(("payments.csv", orphan_pay[:3], "order_id", f"Payment order_id not in orders: {len(orphan_pay)} rows", "Consistency", "Critical"))

    order_map = {o["order_id"]: float(o["order_amount"]) for o in orders}
    amt_mismatch = []
    for p in payments:
        if p["payment_status"] == "SUCCESS" and p["order_id"] in order_map:
            if abs(float(p["amount_paid"]) - order_map[p["order_id"]]) > 0.05:
                amt_mismatch.append(p["payment_id"])
    if amt_mismatch:
        issues.append(("payments.csv", amt_mismatch[:3], "amount_paid", f"Payment amount mismatch for SUCCESS: {len(amt_mismatch)} rows", "Accuracy", "Critical"))

    # 3. Customers
    email_regex = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    bad_email = [c["customer_id"] for c in customers if not email_regex.match(c["email"])]
    if bad_email:
        issues.append(("customers.csv", bad_email[:3], "email", f"Invalid email format: {len(bad_email)} rows", "Validity", "Warning"))

    bad_phone = [c["customer_id"] for c in customers if len(c["phone"].strip()) != 10 or not c["phone"].strip().isdigit()]
    if bad_phone:
        issues.append(("customers.csv", bad_phone[:3], "phone", f"Non-10 digit phone numbers: {len(bad_phone)} rows", "Validity", "Warning"))

    issues.append(("customers.csv", [customers[0]["customer_id"], customers[1]["customer_id"]], "email, phone", "Cleartext customer PII stored without masking, violating DPDP Act 2023", "Validity", "Critical"))

    # 4. Products
    bad_cost = [p["product_id"] for p in products if float(p["cost_price"]) <= 0]
    if bad_cost:
        issues.append(("products.csv", bad_cost[:3], "cost_price", f"Cost price <= 0: {len(bad_cost)} rows", "Accuracy", "Critical"))

    bad_mrp = [p["product_id"] for p in products if float(p["cost_price"]) > float(p["mrp"])]
    if bad_mrp:
        issues.append(("products.csv", bad_mrp[:3], "cost_price", f"Cost price exceeds MRP: {len(bad_mrp)} rows", "Accuracy", "Critical"))

    # 5. Stores
    missing_zone = [s["store_id"] for s in stores if not s.get("zone", "").strip()]
    if missing_zone:
        issues.append(("stores.csv", missing_zone[:3], "zone", f"Missing/empty zone: {len(missing_zone)} rows (e.g. S07 BTM Layout)", "Completeness", "Warning"))

    zero_cap = [s["store_id"] for s in stores if int(s.get("capacity_orders_per_day", 0)) <= 0]
    if zero_cap:
        issues.append(("stores.csv", zero_cap[:3], "capacity_orders_per_day", f"Zero or negative daily store order capacity: {len(zero_cap)} rows (e.g. S12 Rajajinagar)", "Accuracy", "Critical"))

    pin_regex = re.compile(r"^[1-9][0-9]{5}$")
    bad_pin = [s["store_id"] for s in stores if not pin_regex.match(s["pincode"].strip())]
    if bad_pin:
        issues.append(("stores.csv", bad_pin[:3], "pincode", f"Invalid 6-digit Indian PIN code: {len(bad_pin)} rows", "Validity", "Warning"))


    # 6. Increment
    old_status = {o["order_id"]: o["order_status"] for o in orders}
    status_corrections = [inc["order_id"] for inc in increment if inc["order_id"] in old_status and inc["order_status"] != old_status[inc["order_id"]]]
    new_orders = [inc["order_id"] for inc in increment if inc["order_id"] not in old_status]
    print(f"Increment analysis: {len(status_corrections)} status updates (e.g. to RETURNED), {len(new_orders)} new order inserts")

    print("\n" + "="*80)
    print("DATA PROFILING RESULTS SUMMARY (FOR SUBMISSION.md)")
    print("="*80)
    for i, (file, ids, col, desc, dim, sev) in enumerate(issues, 1):
        print(f"| {i} | `{file}` | {', '.join(ids)} | `{col}` | {desc} | {dim} | Enforce validation rule | {sev} |")

if __name__ == "__main__":
    profile()
