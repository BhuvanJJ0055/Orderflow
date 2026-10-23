"""Unit tests for pipeline reconciliation arithmetic."""


def check_reconciliation(summary: dict[str, dict[str, int]]) -> bool:
    """Validate that rows_in equals duplicates + rejected + loaded for every table."""
    for counts in summary.values():
        expected = counts["duplicates"] + counts["rejected"] + counts["loaded"]
        if counts["rows_in"] != expected:
            return False
    return True


def test_reconciliation_exact_match() -> None:
    summary = {
        "orders": {"rows_in": 1000, "duplicates": 10, "rejected": 5, "loaded": 985},
        "customers": {"rows_in": 500, "duplicates": 0, "rejected": 2, "loaded": 498},
        "order_items": {"rows_in": 2000, "duplicates": 20, "rejected": 10, "loaded": 1970},
        "products": {"rows_in": 300, "duplicates": 5, "rejected": 1, "loaded": 294},
        "payments": {"rows_in": 1200, "duplicates": 15, "rejected": 3, "loaded": 1182},
    }
    assert check_reconciliation(summary) is True


def test_reconciliation_mismatch_detected() -> None:
    summary = {
        "orders": {"rows_in": 1000, "duplicates": 10, "rejected": 5, "loaded": 980},  # sum is 995 != 1000
    }
    assert check_reconciliation(summary) is False
