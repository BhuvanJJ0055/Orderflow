"""Unit tests for data transformations and pure filtering logic."""

from datetime import datetime

import pandas as pd

from src.etl.transform import (
    clean_customers,
    clean_orders,
    clean_payments,
    clean_products,
    filter_incremental_orders,
)


def test_orders_cleaning_dedupe_dates_and_status() -> None:
    df = pd.DataFrame({
        "order_id": ["o1", "o1", "o2"],
        "order_status": [" DELIVERED ", "shipped", "Invoiced "],
        "order_purchase_timestamp": ["2017-01-01 10:00:00", "2017-01-01 10:00:00", "invalid-date"],
    })
    cleaned, dups = clean_orders(df)
    assert dups == 1
    assert len(cleaned) == 2
    assert cleaned.loc[cleaned["order_id"] == "o1", "order_status"].iloc[0] == "delivered"
    assert cleaned.loc[cleaned["order_id"] == "o2", "order_status"].iloc[0] == "invoiced"
    assert pd.isna(cleaned.loc[cleaned["order_id"] == "o2", "order_purchase_timestamp"].iloc[0])


def test_customers_cleaning() -> None:
    df = pd.DataFrame({
        "customer_id": ["c1", "c1", "c2"],
        "customer_city": [" sao paulo ", "sao paulo", "rio de janeiro"],
        "customer_state": [" sp ", "sp", "rj"],
    })
    cleaned, dups = clean_customers(df)
    assert dups == 1
    assert len(cleaned) == 2
    assert cleaned.loc[cleaned["customer_id"] == "c1", "customer_city"].iloc[0] == "Sao Paulo"
    assert cleaned.loc[cleaned["customer_id"] == "c1", "customer_state"].iloc[0] == "SP"


def test_products_cleaning() -> None:
    df = pd.DataFrame({
        "product_id": ["p1", "p1", "p2"],
        "product_category_name": [" BELEZA_SAUDE ", "beleza_saude", None],
        "product_weight_g": [500, 500, 1000],
        "extra_column": ["ignore_me", "ignore_me", "ignore_me"],
    })
    cleaned, dups = clean_products(df)
    assert dups == 1
    assert "extra_column" not in cleaned.columns
    assert cleaned.loc[cleaned["product_id"] == "p1", "product_category_name"].iloc[0] == "beleza_saude"
    assert cleaned.loc[cleaned["product_id"] == "p2", "product_category_name"].iloc[0] == "unknown"


def test_payments_cleaning() -> None:
    df = pd.DataFrame({
        "order_id": ["o1", "o1", "o1"],
        "payment_sequential": [1, 1, 2],
        "payment_type": [" CREDIT_CARD ", "credit_card", " BOLETO "],
    })
    cleaned, dups = clean_payments(df)
    assert dups == 1
    assert len(cleaned) == 2
    assert cleaned.loc[cleaned["payment_sequential"] == 1, "payment_type"].iloc[0] == "credit_card"
    assert cleaned.loc[cleaned["payment_sequential"] == 2, "payment_type"].iloc[0] == "boleto"


def test_incremental_filter_logic() -> None:
    df = pd.DataFrame({
        "order_id": ["o1", "o2", "o3", "o4"],
        "order_purchase_timestamp": [
            pd.Timestamp("2018-01-01"),
            pd.Timestamp("2018-02-01"),
            pd.Timestamp("2018-03-01"),
            pd.Timestamp("2018-04-01"),
        ],
    })

    # Test watermark with anti-join
    watermark = datetime(2018, 2, 1)
    existing_ids = {"o2"}
    filtered = filter_incremental_orders(df, watermark=watermark, existing_order_ids=existing_ids)

    # o1 is before watermark; o2 is equal to watermark but in existing_ids; o3 and o4 are new
    assert set(filtered["order_id"]) == {"o3", "o4"}

    # Test cutoff
    cutoff_filtered = filter_incremental_orders(df, cutoff="2018-03-01")
    assert set(cutoff_filtered["order_id"]) == {"o1", "o2"}
