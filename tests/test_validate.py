"""Unit tests for validation rules and quarantine logic."""

import pandas as pd

from src.etl.validate import apply_rules


def test_order_items_invalid_price_rejected() -> None:
    df = pd.DataFrame({
        "order_id": ["o1", "o2", "o3", "o4"],
        "order_item_id": [1, 1, 1, 1],
        "price": [10.5, 0.0, -5.0, None],
        "freight_value": [2.0, 2.0, 2.0, 2.0],
    })
    valid, rejected = apply_rules(df, "order_items")
    assert len(valid) == 1
    assert valid["order_id"].iloc[0] == "o1"
    assert len(rejected) == 3
    for reason in rejected["reject_reason"]:
        assert "invalid_price" in reason


def test_orders_rules_tiny_frame() -> None:
    df = pd.DataFrame({
        "order_id": ["o1", "o2", "o3"],
        "order_purchase_timestamp": [pd.Timestamp("2018-01-01"), None, pd.Timestamp("2018-01-02")],
        "order_status": ["delivered", "delivered", "unknown_status"],
    })
    valid, rejected = apply_rules(df, "orders")
    assert len(valid) == 1
    assert valid["order_id"].iloc[0] == "o1"
    assert len(rejected) == 2
    reasons = dict(zip(rejected["order_id"], rejected["reject_reason"], strict=False))
    assert "missing_purchase_timestamp" in reasons["o2"]
    assert "invalid_order_status" in reasons["o3"]


def test_customers_rules_tiny_frame() -> None:
    df = pd.DataFrame({
        "customer_id": ["c1", None, "c3"],
        "customer_unique_id": ["u1", "u2", ""],
    })
    valid, rejected = apply_rules(df, "customers")
    assert len(valid) == 1
    assert valid["customer_id"].iloc[0] == "c1"
    assert len(rejected) == 2


def test_products_rules_tiny_frame() -> None:
    df = pd.DataFrame({
        "product_id": ["p1", None, "  "],
        "product_category_name": ["cat1", "cat2", "cat3"],
    })
    valid, rejected = apply_rules(df, "products")
    assert len(valid) == 1
    assert valid["product_id"].iloc[0] == "p1"
    assert len(rejected) == 2


def test_payments_rules_tiny_frame() -> None:
    df = pd.DataFrame({
        "order_id": ["o1", "o2"],
        "payment_sequential": [1, 1],
        "payment_value": [50.0, -10.0],
    })
    valid, rejected = apply_rules(df, "payments")
    assert len(valid) == 1
    assert valid["order_id"].iloc[0] == "o1"
    assert len(rejected) == 1
    assert "negative_payment_value" in rejected["reject_reason"].iloc[0]
