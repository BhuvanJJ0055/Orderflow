"""Data validation module using rule dictionaries to quarantine bad rows."""

from collections.abc import Callable

import pandas as pd

VALID_ORDER_STATUSES = {
    "created",
    "approved",
    "invoiced",
    "processing",
    "shipped",
    "delivered",
    "unavailable",
    "canceled",
}


def _check_missing_purchase_ts(df: pd.DataFrame) -> pd.Series:
    return df["order_purchase_timestamp"].isna()


def _check_invalid_order_status(df: pd.DataFrame) -> pd.Series:
    return ~df["order_status"].isin(VALID_ORDER_STATUSES)


def _check_missing_customer_id(df: pd.DataFrame) -> pd.Series:
    return df["customer_id"].isna() | (df["customer_id"].astype(str).str.strip() == "")


def _check_missing_customer_unique_id(df: pd.DataFrame) -> pd.Series:
    return df["customer_unique_id"].isna() | (df["customer_unique_id"].astype(str).str.strip() == "")


def _check_invalid_price(df: pd.DataFrame) -> pd.Series:
    numeric_price = pd.to_numeric(df["price"], errors="coerce")
    return numeric_price.isna() | (numeric_price <= 0)


def _check_invalid_freight(df: pd.DataFrame) -> pd.Series:
    numeric_freight = pd.to_numeric(df["freight_value"], errors="coerce")
    return numeric_freight < 0


def _check_missing_product_id(df: pd.DataFrame) -> pd.Series:
    return df["product_id"].isna() | (df["product_id"].astype(str).str.strip() == "")


def _check_negative_payment_value(df: pd.DataFrame) -> pd.Series:
    numeric_val = pd.to_numeric(df["payment_value"], errors="coerce")
    return numeric_val < 0


TABLE_RULES: dict[str, dict[str, Callable[[pd.DataFrame], pd.Series]]] = {
    "orders": {
        "missing_purchase_timestamp": _check_missing_purchase_ts,
        "invalid_order_status": _check_invalid_order_status,
    },
    "customers": {
        "missing_customer_id": _check_missing_customer_id,
        "missing_customer_unique_id": _check_missing_customer_unique_id,
    },
    "order_items": {
        "invalid_price": _check_invalid_price,
        "invalid_freight": _check_invalid_freight,
    },
    "products": {
        "missing_product_id": _check_missing_product_id,
    },
    "payments": {
        "negative_payment_value": _check_negative_payment_value,
    },
}


def apply_rules(df: pd.DataFrame, table_name: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate table-specific validation rules.

    Returns:
        (valid_df, rejected_df): where rejected_df contains quarantined rows
        with an added 'reject_reason' column.
    """
    if table_name not in TABLE_RULES:
        raise ValueError(f"Unknown validation rules for table '{table_name}'")

    if df.empty:
        empty_rejects = df.copy()
        empty_rejects["reject_reason"] = pd.Series(dtype=str)
        return df.copy(), empty_rejects

    rules = TABLE_RULES[table_name]
    reasons_per_row = pd.Series("", index=df.index, dtype=object)
    any_bad = pd.Series(False, index=df.index)

    for reason_name, rule_func in rules.items():
        bad_mask = rule_func(df).fillna(True)
        any_bad = any_bad | bad_mask
        # Accumulate reject reasons for failed checks
        matching_indices = df.index[bad_mask]
        for idx in matching_indices:
            current = reasons_per_row.at[idx]
            reasons_per_row.at[idx] = f"{current};{reason_name}" if current else reason_name

    valid_df = df[~any_bad].copy()
    rejected_df = df[any_bad].copy()
    rejected_df["reject_reason"] = reasons_per_row[any_bad]

    return valid_df, rejected_df
