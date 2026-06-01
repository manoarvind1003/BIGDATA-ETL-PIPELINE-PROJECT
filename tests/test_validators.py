from __future__ import annotations

import pytest
from typing import Dict, Any

from utils.validators import validate_order, validate_batch


def _base_order(**overrides) -> Dict[str, Any]:
    """Return a valid order dict with optional field overrides."""
    order: Dict[str, Any] = {
        "order_id": 1001,
        "customer_id": 501,
        "restaurant_id": 12,
        "city": "Mumbai",
        "item_name": "Chicken Biryani",
        "quantity": 2,
        "amount": 450.00,
        "payment_mode": "UPI",
        "delivery_status": "Delivered",
        "order_time": "2026-06-01 14:35:00",
    }
    order.update(overrides)
    return order


# ─────────────────────────────────────────────────────────────────────────────
# Valid record
# ─────────────────────────────────────────────────────────────────────────────

class TestValidOrder:
    def test_valid_order_passes(self):
        result = validate_order(_base_order())
        assert result.is_valid is True
        assert result.errors == []

    def test_valid_order_returns_original_record(self):
        order = _base_order()
        result = validate_order(order)
        assert result.record == order


# ─────────────────────────────────────────────────────────────────────────────
# Amount rules
# ─────────────────────────────────────────────────────────────────────────────

class TestAmountValidation:
    def test_negative_amount_fails(self):
        result = validate_order(_base_order(amount=-100))
        assert result.is_valid is False
        assert any("amount" in e for e in result.errors)

    def test_zero_amount_fails(self):
        result = validate_order(_base_order(amount=0))
        assert result.is_valid is False

    def test_string_amount_fails(self):
        result = validate_order(_base_order(amount="not-a-number"))
        assert result.is_valid is False

    def test_none_amount_fails(self):
        result = validate_order(_base_order(amount=None))
        assert result.is_valid is False

    def test_very_large_amount_passes(self):
        result = validate_order(_base_order(amount=999999.99))
        assert result.is_valid is True


# ─────────────────────────────────────────────────────────────────────────────
# Quantity rules
# ─────────────────────────────────────────────────────────────────────────────

class TestQuantityValidation:
    def test_zero_quantity_fails(self):
        result = validate_order(_base_order(quantity=0))
        assert result.is_valid is False
        assert any("quantity" in e for e in result.errors)

    def test_negative_quantity_fails(self):
        result = validate_order(_base_order(quantity=-3))
        assert result.is_valid is False

    def test_string_quantity_fails(self):
        result = validate_order(_base_order(quantity="two"))
        assert result.is_valid is False

    def test_quantity_one_passes(self):
        result = validate_order(_base_order(quantity=1))
        assert result.is_valid is True


# ─────────────────────────────────────────────────────────────────────────────
# City rules
# ─────────────────────────────────────────────────────────────────────────────

class TestCityValidation:
    def test_null_city_fails(self):
        result = validate_order(_base_order(city=None))
        assert result.is_valid is False

    def test_empty_city_fails(self):
        result = validate_order(_base_order(city=""))
        assert result.is_valid is False

    def test_whitespace_city_fails(self):
        result = validate_order(_base_order(city="   "))
        assert result.is_valid is False


# ─────────────────────────────────────────────────────────────────────────────
# item_name rules
# ─────────────────────────────────────────────────────────────────────────────

class TestItemNameValidation:
    def test_null_item_name_fails(self):
        result = validate_order(_base_order(item_name=None))
        assert result.is_valid is False

    def test_empty_item_name_fails(self):
        result = validate_order(_base_order(item_name=""))
        assert result.is_valid is False


# ─────────────────────────────────────────────────────────────────────────────
# Missing field rules
# ─────────────────────────────────────────────────────────────────────────────

class TestMissingFields:
    def test_missing_order_id_fails(self):
        order = _base_order()
        del order["order_id"]
        result = validate_order(order)
        assert result.is_valid is False

    def test_empty_record_fails(self):
        result = validate_order({})
        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_multiple_errors_collected(self):
        """Both amount and quantity invalid — both errors must be reported."""
        result = validate_order(_base_order(amount=-1, quantity=0))
        assert result.is_valid is False
        assert len(result.errors) >= 2


# ─────────────────────────────────────────────────────────────────────────────
# validate_batch
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateBatch:
    def test_all_valid(self):
        records = [_base_order(order_id=i) for i in range(1, 6)]
        valid, invalid = validate_batch(records)
        assert len(valid) == 5
        assert len(invalid) == 0

    def test_all_invalid(self):
        records = [_base_order(order_id=i, amount=-1) for i in range(1, 4)]
        valid, invalid = validate_batch(records)
        assert len(valid) == 0
        assert len(invalid) == 3

    def test_mixed_batch(self):
        records = [
            _base_order(order_id=1),             # valid
            _base_order(order_id=2, amount=0),   # invalid
            _base_order(order_id=3),             # valid
            _base_order(order_id=4, city=None),  # invalid
        ]
        valid, invalid = validate_batch(records)
        assert len(valid) == 2
        assert len(invalid) == 2

    def test_invalid_records_have_error_field(self):
        records = [_base_order(order_id=1, amount=-5)]
        _, invalid = validate_batch(records)
        assert "_validation_errors" in invalid[0]
        assert len(invalid[0]["_validation_errors"]) > 0
