from __future__ import annotations

import pytest
from datetime import datetime
from typing import Any, Dict

from producer.order_generator import generate_order, generate_invalid_order
from config import producer_config


class TestGenerateOrder:
    """Test suite for the generate_order() function."""

    def test_returns_dict(self):
        order = generate_order()
        assert isinstance(order, dict)

    def test_required_fields_present(self):
        required = [
            "order_id", "customer_id", "restaurant_id", "city",
            "item_name", "quantity", "amount", "payment_mode",
            "delivery_status", "order_time",
        ]
        order = generate_order()
        for field in required:
            assert field in order, f"Missing field: {field}"

    def test_order_id_positive(self):
        order = generate_order()
        assert order["order_id"] > 0

    def test_amount_positive(self):
        order = generate_order()
        assert order["amount"] > 0

    def test_quantity_positive(self):
        order = generate_order()
        assert order["quantity"] >= 1

    def test_city_valid(self):
        order = generate_order()
        assert order["city"] in producer_config.cities

    def test_item_name_valid(self):
        order = generate_order()
        assert order["item_name"] in producer_config.food_items

    def test_payment_mode_valid(self):
        order = generate_order()
        assert order["payment_mode"] in producer_config.payment_modes

    def test_delivery_status_valid(self):
        order = generate_order()
        assert order["delivery_status"] in producer_config.delivery_statuses

    def test_order_time_format(self):
        order = generate_order()
        # Should be parseable as datetime
        dt = datetime.strptime(order["order_time"], "%Y-%m-%d %H:%M:%S")
        assert dt is not None

    def test_amount_equals_quantity_times_unit_price(self):
        """amount should always be quantity × unit_price (within float precision)."""
        for _ in range(20):
            order = generate_order()
            # amount >= quantity × min_price  AND  amount <= quantity × max_price
            min_amt = order["quantity"] * producer_config.price_range["min"]
            max_amt = order["quantity"] * producer_config.price_range["max"]
            assert min_amt <= order["amount"] <= max_amt + 0.01

    def test_order_ids_are_unique_across_calls(self):
        ids = {generate_order()["order_id"] for _ in range(50)}
        assert len(ids) == 50, "Duplicate order IDs detected"

    def test_customer_id_range(self):
        for _ in range(10):
            order = generate_order()
            assert 500 <= order["customer_id"] <= 9999

    def test_restaurant_id_range(self):
        for _ in range(10):
            order = generate_order()
            assert 1 <= order["restaurant_id"] <= 200


class TestGenerateInvalidOrder:
    """Test suite for the generate_invalid_order() function (fault injection)."""

    def test_returns_dict(self):
        order = generate_invalid_order()
        assert isinstance(order, dict)

    def test_has_at_least_one_invalid_field(self):
        """
        One of: amount <= 0, quantity <= 0, or city is None.
        """
        for _ in range(30):
            order = generate_invalid_order()
            is_invalid = (
                (order.get("amount") is not None and order["amount"] <= 0)
                or (order.get("quantity") is not None and order["quantity"] <= 0)
                or order.get("city") is None
            )
            assert is_invalid, f"Expected invalid order, got: {order}"
