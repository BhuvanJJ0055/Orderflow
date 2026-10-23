"""Data transformation module providing pure cleaning functions."""

import pandas as pd


def clean_orders(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deduplicate on order_id, parse timestamp columns, and normalize order_status."""
    initial_count = len(df)
    clean_df = df.drop_duplicates(subset=["order_id"]).copy()
    duplicates_removed = initial_count - len(clean_df)

    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_columns:
        if col in clean_df.columns:
            clean_df[col] = pd.to_datetime(clean_df[col], errors="coerce")

    if "order_status" in clean_df.columns:
        clean_df["order_status"] = clean_df["order_status"].astype(str).str.strip().str.lower()

    return clean_df, duplicates_removed


def clean_customers(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deduplicate on customer_id, title-case city, and uppercase state."""
    initial_count = len(df)
    clean_df = df.drop_duplicates(subset=["customer_id"]).copy()
    duplicates_removed = initial_count - len(clean_df)

    if "customer_city" in clean_df.columns:
        clean_df["customer_city"] = clean_df["customer_city"].astype(str).str.strip().str.title()
    if "customer_state" in clean_df.columns:
        clean_df["customer_state"] = clean_df["customer_state"].astype(str).str.strip().str.upper()

    return clean_df, duplicates_removed


def clean_order_items(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deduplicate on composite key (order_id, order_item_id) and parse shipping_limit_date."""
    initial_count = len(df)
    clean_df = df.drop_duplicates(subset=["order_id", "order_item_id"]).copy()
    duplicates_removed = initial_count - len(clean_df)

    if "shipping_limit_date" in clean_df.columns:
        clean_df["shipping_limit_date"] = pd.to_datetime(clean_df["shipping_limit_date"], errors="coerce")

    return clean_df, duplicates_removed


def clean_products(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deduplicate on product_id, prune columns, and normalize product category."""
    initial_count = len(df)
    clean_df = df.drop_duplicates(subset=["product_id"]).copy()
    duplicates_removed = initial_count - len(clean_df)

    keep_columns = ["product_id", "product_category_name", "product_weight_g"]
    available_cols = [c for c in keep_columns if c in clean_df.columns]
    clean_df = clean_df[available_cols].copy()

    if "product_category_name" in clean_df.columns:
        clean_df["product_category_name"] = (
            clean_df["product_category_name"].fillna("unknown").astype(str).str.strip().str.lower()
        )

    return clean_df, duplicates_removed


def clean_payments(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deduplicate on composite key (order_id, payment_sequential) and normalize payment_type."""
    initial_count = len(df)
    clean_df = df.drop_duplicates(subset=["order_id", "payment_sequential"]).copy()
    duplicates_removed = initial_count - len(clean_df)

    if "payment_type" in clean_df.columns:
        clean_df["payment_type"] = clean_df["payment_type"].astype(str).str.strip().str.lower()

    return clean_df, duplicates_removed


def clean_table(df: pd.DataFrame, table_name: str) -> tuple[pd.DataFrame, int]:
    """Dispatch cleaning for a given table name."""
    dispatch_map = {
        "orders": clean_orders,
        "customers": clean_customers,
        "order_items": clean_order_items,
        "products": clean_products,
        "payments": clean_payments,
    }
    if table_name not in dispatch_map:
        raise ValueError(f"No cleaning routine for table '{table_name}'")
    return dispatch_map[table_name](df)
