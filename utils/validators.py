from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from utils.logger import get_logger

logger = get_logger(__name__)

# Fields that MUST be present in every order message
REQUIRED_FIELDS: List[str] = [
    "order_id",
    "customer_id",
    "restaurant_id",
    "city",
    "item_name",
    "quantity",
    "amount",
    "payment_mode",
    "delivery_status",
    "order_time",
]


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]
    record: Dict[str, Any]


def validate_order(record: Dict[str, Any]) -> ValidationResult:
    """
    Validate a single order record against business rules.

    Rules:
        1. All required fields must be present.
        2. amount must be > 0.
        3. quantity must be > 0.
        4. city must be non-empty string.
        5. item_name must be non-empty string.
        6. order_id must be a positive integer.

    Returns:
        :class:`ValidationResult` with ``is_valid`` flag and ``errors`` list.
    """
    errors: List[str] = []

    # ── 1. Required field presence ────────────────────────────────────────────
    for field in REQUIRED_FIELDS:
        if field not in record or record[field] is None:
            errors.append(f"Missing required field: '{field}'")

    # Stop early if structural issues exist
    if errors:
        return ValidationResult(is_valid=False, errors=errors, record=record)

    # ── 2. Amount > 0 ─────────────────────────────────────────────────────────
    try:
        amount = float(record["amount"])
        if amount <= 0:
            errors.append(f"Invalid amount: {amount} (must be > 0)")
    except (TypeError, ValueError):
        errors.append(f"Non-numeric amount: {record['amount']!r}")

    # ── 3. Quantity > 0 ───────────────────────────────────────────────────────
    try:
        quantity = int(record["quantity"])
        if quantity <= 0:
            errors.append(f"Invalid quantity: {quantity} (must be > 0)")
    except (TypeError, ValueError):
        errors.append(f"Non-integer quantity: {record['quantity']!r}")

    # ── 4. City non-empty ─────────────────────────────────────────────────────
    city = str(record.get("city", "")).strip()
    if not city:
        errors.append("city cannot be empty or whitespace")

    # ── 5. Item name non-empty ────────────────────────────────────────────────
    item_name = str(record.get("item_name", "")).strip()
    if not item_name:
        errors.append("item_name cannot be empty or whitespace")

    # ── 6. Positive order_id ─────────────────────────────────────────────────
    try:
        order_id = int(record["order_id"])
        if order_id <= 0:
            errors.append(f"Invalid order_id: {order_id} (must be > 0)")
    except (TypeError, ValueError):
        errors.append(f"Non-integer order_id: {record['order_id']!r}")

    is_valid = len(errors) == 0

    if not is_valid:
        logger.warning(
            "Validation failed for order_id=%s | Errors: %s",
            record.get("order_id", "UNKNOWN"),
            "; ".join(errors),
        )

    return ValidationResult(is_valid=is_valid, errors=errors, record=record)


def validate_batch(
    records: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Partition a list of records into valid and invalid sets.

    Args:
        records: Raw records to validate.

    Returns:
        (valid_records, invalid_records) tuple.
    """
    valid: List[Dict[str, Any]] = []
    invalid: List[Dict[str, Any]] = []

    for record in records:
        result = validate_order(record)
        if result.is_valid:
            valid.append(record)
        else:
            enriched = dict(record)
            enriched["_validation_errors"] = "; ".join(result.errors)
            invalid.append(enriched)

    logger.info(
        "Batch validation complete | Valid: %d | Invalid: %d",
        len(valid),
        len(invalid),
    )
    return valid, invalid
