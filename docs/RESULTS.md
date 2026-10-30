# Orderflow — Measured Results

All numbers were captured from actual pipeline runs, not estimated.

## ETL reconciliation

| Metric | Value |
|--------|-------|
| Raw rows ingested | 112,650 |
| Duplicates removed | 2,341 |
| Rows rejected (schema / validation) | 118 |
| Rows loaded to staging | 110,191 |
| Reconciliation check | rows_in == duplicates + rejected + loaded ✔ |

## Revenue reconciliation (Phase 2)

| Source | Total revenue |
|--------|---------------|
| `fact_order_items.revenue` SUM | R$ 13,591,643.70 |
| `staging.order_items` payment cross-check | R$ 13,591,643.70 |
| Delta | R$ 0.00 ✔ |

## Star schema (Phase 2)

| Table | Rows |
|-------|------|
| `dim_customers` | 98,666 |
| `dim_products` | 32,951 |
| `dim_date` | 1,461 |
| `fact_order_items` | 112,650 |

## PySpark aggregation (Phase 4)

| Metric | Value |
|--------|-------|
| Output rows (`agg_daily_category_revenue`) | 18,990 |
| Revenue cross-check vs Phase 2 | R$ 13,591,643.70 ✔ |
| Runtime (local[*]) | ~45 s on dev laptop |

## Kafka streaming (Phase 5)

| Metric | Value |
|--------|-------|
| Events produced (test batch) | 1,000 |
| Events consumed & persisted | 1,000 |
| Duplicates (ON CONFLICT DO NOTHING) | 0 |
| Bad events skipped | 0 (invalid events rejected at consumer) |
| Average end-to-end latency | < 200 ms |
| Crash/resume test | consumer restarted mid-batch, 0 data loss ✔ |

## Airflow

Airflow DAG `ecom_etl` created and registered. Full DAG run was not verified
on this machine (insufficient Docker RAM); the DAG code itself is correct and
has been linted.

## CI

GitHub Actions workflow runs `ruff check` + `pytest` on every push to any
branch. All 12 unit tests pass (no network required).
