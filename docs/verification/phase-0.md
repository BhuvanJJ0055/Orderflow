# Phase 0: Preflight and Environment Verification

- **OS**: Windows 10 (10.0.26300)
- **CPU Count**: 12
- **Python Version**: 3.11.6 (tags/v3.11.6:8b6ee5b, Oct  2 2023, 14:57:12) [MSC v.1935 64 bit (AMD64)]

| Check | Result | Evidence |
| --- | --- | --- |
| Git CLI | PASS | git version 2.51.2.windows.1 |
| Python 3.10+ | PASS | Python 3.11.6 |
| Docker & Compose | PASS | Daemon running; Docker Compose version v5.6.0 |
| Java (11 or 17) | PASS | openjdk version "17.0.16" 2025-07-15 |
| GitHub CLI (optional) | PASS | Not installed (fallback to git merge) |
| Git remote reachable | PASS | 942379013af95c3ffc32aa5f6c7ed34ff6d8c98f	HEAD |
| .env not tracked | PASS | Not tracked by git |
| Five raw CSV datasets exist | PASS | olist_orders_dataset.csv (99442 rows); olist_customers_dataset.csv (99442 rows); olist_order_items_dataset.csv (112651 rows); olist_products_dataset.csv (32952 rows); olist_order_payments_dataset.csv (103887 rows) |
