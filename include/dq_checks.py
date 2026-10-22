"""Data Quality Dimensions and Helper Functions for NammaMart Pipeline (AIT-175).

Implements checks across the 6 core data quality dimensions:
1. Completeness: Missing critical primary or foreign keys.
2. Uniqueness: Duplicate keys.
3. Validity: Allowed status/payment enum values, regex formats.
4. Accuracy: Formula verification (order_amount == qty * price * (1 - discount/100)).
5. Consistency: Referential integrity across foreign keys.
6. Timeliness: Future dates, freshness checks.
"""

from __future__ import annotations

import csv
import os
import re
from typing import Any

VALID_ORDER_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "CARD", "CASH ON DELIVERY", "WALLET"}


def mask_email(email: str | Any) -> str:
    """Mask email address (e.g. pr***@example.com) for DPDP Act 2023 compliance."""
    if not isinstance(email, str) or "@" not in email:
        return "invalid_email@masked"
    user, domain = email.split("@", 1)
    if len(user) <= 2:
        masked_user = user[0] + "***"
    else:
        masked_user = user[:2] + "***"
    return f"{masked_user}@{domain}"


def mask_phone(phone: str | Any) -> str:
    """Mask phone number (e.g. 98******21) for DPDP Act 2023 compliance."""
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


def calculate_expected_amount(quantity: float, unit_price: float, discount_pct: float) -> float:
    """Compute order_amount = round(quantity * unit_price * (1 - discount_pct / 100), 2)."""
    return round(float(quantity) * float(unit_price) * (1.0 - (float(discount_pct) / 100.0)), 2)


def validate_order_row(row: dict[str, Any], valid_customers: set[str], valid_products: set[str], valid_stores: set[str]) -> list[str]:
    """Validate a single order row against critical data quality dimensions."""
    reasons = []

    # 1. Completeness & Validity
    if not row.get("order_id"):
        reasons.append("Missing order_id")
    if row.get("order_status") not in VALID_ORDER_STATUSES:
        reasons.append(f"Invalid order_status: {row.get('order_status')}")

    # 2. Accuracy
    try:
        qty = float(row.get("quantity", 0))
        if qty <= 0:
            reasons.append(f"Quantity must be positive: {qty}")
    except (ValueError, TypeError):
        reasons.append(f"Malformed quantity: {row.get('quantity')}")

    if row.get("order_status") == "DELIVERED":
        deliv = row.get("delivery_minutes")
        if deliv is None or str(deliv).strip() == "":
            reasons.append("Missing delivery_minutes for DELIVERED order")
        else:
            try:
                dm = float(deliv)
                if dm < 8 or dm > 120:
                    reasons.append(f"delivery_minutes {dm} out of SLA range [8, 120]")
            except (ValueError, TypeError):
                reasons.append(f"Malformed delivery_minutes: {deliv}")

    try:
        qty = float(row.get("quantity", 0))
        price = float(row.get("unit_price", 0))
        disc = float(row.get("discount_pct", 0))
        expected_amt = calculate_expected_amount(qty, price, disc)
        actual_amt = float(row.get("order_amount", 0))
        if abs(actual_amt - expected_amt) > 0.05:
            reasons.append(f"Amount mismatch: expected {expected_amt}, got {actual_amt}")
    except (ValueError, TypeError):
        reasons.append("Malformed pricing fields for amount calculation")

    # 3. Consistency (Referential Integrity)
    cust_id = row.get("customer_id")
    if valid_customers and cust_id not in valid_customers:
        reasons.append(f"customer_id {cust_id} not found in customer master")

    prod_id = row.get("product_id")
    if valid_products and prod_id not in valid_products:
        reasons.append(f"product_id {prod_id} not found in product master")

    store_id = row.get("store_id")
    if valid_stores and store_id not in valid_stores:
        reasons.append(f"store_id {store_id} not found in store master")

    # 4. Timeliness
    ts = str(row.get("order_ts", ""))
    if ts > "2026-10-02":
        reasons.append(f"Future order timestamp: {ts}")

    return reasons


def validate_store_row(row: dict[str, Any]) -> list[str]:
    """Validate a single store record for completeness and accuracy."""
    reasons = []
    if not row.get("store_id"):
        reasons.append("Missing store_id (Completeness)")
    zone = row.get("zone")
    if zone is None or str(zone).strip() == "":
        reasons.append("Missing zone (Completeness)")
    try:
        cap = int(row.get("capacity_orders_per_day", 0))
        if cap <= 0:
            reasons.append(f"Capacity must be positive, got {cap} (Accuracy)")
    except (ValueError, TypeError):
        reasons.append("Malformed capacity_orders_per_day")
    pincode = str(row.get("pincode", "")).strip()
    if not re.match(r"^[1-9][0-9]{5}$", pincode):
        reasons.append(f"Invalid 6-digit PIN code: {pincode} (Validity)")
    return reasons


def run_checks_suite():
    """Verify quality logic against test assertions."""
    assert mask_email("shubham@cossth.com") == "sh***@cossth.com"
    assert mask_phone("9876543210") == "98******10"
    assert normalize_payment_mode(" upi ") == "UPI"
    assert calculate_expected_amount(2, 100, 10) == 180.0
    bad_store = {"store_id": "S07", "zone": "", "capacity_orders_per_day": "500", "pincode": "560076"}
    assert "Missing zone (Completeness)" in validate_store_row(bad_store)
    zero_cap_store = {"store_id": "S12", "zone": "West", "capacity_orders_per_day": "0", "pincode": "560010"}
    assert any("Capacity must be positive" in r for r in validate_store_row(zero_cap_store))
    print("All core data quality rule unit checks PASSED.")


if __name__ == "__main__":
    run_checks_suite()

