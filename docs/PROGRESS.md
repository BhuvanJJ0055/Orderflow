# Project Progress

## Summary
- Dataset: Real Olist Brazilian E-Commerce dataset (99,441 orders, 112,650 items, 32,951 products, 103,886 payments, 99,441 customers)
- Target Repository: https://github.com/BhuvanJJ0055/Orderflow.git

## Phase Status

| Phase | Description | Branch | Tag | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Phase 0 | Preflight and repo setup | `main` | N/A | Completed | Docker daemon active, Python 3.11, OpenJDK 17, Olist CSVs present |
| Phase 1 | Python + PostgreSQL ETL | `phase-1-etl` | `v0.1-phase1` | Completed | 5 tables loaded into staging; exact reconciliation; idempotent 2-run verified |
| Phase 2 | SQL data modelling | `phase-2-model` | `v0.2-phase2` | Completed | Star schema built; 112,650 fact items; exact independent revenue reconciliation (R$ 13,591,643.70); indexes verified |
| Phase 3 | Automation, tests, CI, Airflow | `phase-3-automation` | `v0.3-phase3` | Completed | Unit tests passed, incremental + rollback verified, Airflow NOT VERIFIED (Docker RAM constraint) |
| Phase 4 | PySpark daily revenue | `phase-4-pyspark` | `v0.4-phase4` | Not Started | |
| Phase 5 | Kafka streaming | `phase-5-kafka` | `v0.5-phase5` | Not Started | |
| Phase 6 | Docker, dashboard, polish | `phase-6-polish` | `v1.0.0` | Not Started | |

## Measured Numbers (Phase 1, 2 & 3)
- `staging.orders`: 99,441 rows loaded
- `staging.customers`: 99,441 rows loaded
- `staging.order_items`: 112,650 rows loaded
- `staging.products`: 32,951 rows loaded
- `staging.payments`: 103,886 rows loaded
- `analytics.dim_customers`: 96,096 unique customer entities
- `analytics.dim_products`: 32,951 unique products
- `analytics.dim_date`: 1,461 calendar date records (2016 to 2019)
- `analytics.fact_order_items`: 112,650 order items (0 orphans, 0 duplicate grain)
- Total Fact Revenue: R$ 13,591,643.70 (exact independent reconciliation, 0.00 gap)
- Cutoff load: 80,450 orders (91,231 fact items) loaded with `--cutoff 2018-06-01`
- Incremental load: 18,991 orders appended transactionally (watermark moved to 2018-10-17 17:30:18)
- Incremental idempotency: 0 new rows on re-run, watermark unchanged
- Crash injection rollback: failed run logged in `ops.pipeline_runs`, 0 half-loaded rows committed
- Unreachable DB handling: logged failure to `logs/failed_runs.jsonl`
- Execution speed: Full load 83.44s (median); Incremental load 23.46s (median)
- Unit tests: 12 tests passed, 0 failures, ruff clean

