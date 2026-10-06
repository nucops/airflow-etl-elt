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

import re
from typing import Any

VALID_ORDER_STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_PAYMENT_MODES = {"UPI", "CARD", "CASH ON DELIVERY", "WALLET"}


def mask_email(email: str | Any) -> str:
    """Mask email address (e.g. pr***@example.com)."""
    if not isinstance(email, str) or "@" not in email:
        return "invalid_email@masked"
    user, domain = email.split("@", 1)
    if len(user) <= 2:
        masked_user = user[0] + "***"
    else:
        masked_user = user[:2] + "***"
    return f"{masked_user}@{domain}"


def mask_phone(phone: str | Any) -> str:
    """Mask phone number (e.g. 98******21)."""
    s = str(phone).strip()
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 10:
        return f"{digits[:2]}******{digits[-2:]}"
    return "******"


def calculate_expected_amount(quantity: float, unit_price: float, discount_pct: float) -> float:
    """Compute order_amount = round(quantity * unit_price * (1 - discount_pct / 100), 2)."""
    return round(float(quantity) * float(unit_price) * (1.0 - (float(discount_pct) / 100.0)), 2)
