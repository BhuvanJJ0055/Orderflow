# Project Progress

## Summary
- Dataset: Real Olist Brazilian E-Commerce dataset (99,441 orders, 112,650 items, 32,951 products, 103,886 payments, 99,441 customers)
- Target Repository: https://github.com/BhuvanJJ0055/Orderflow.git

## Phase Status

| Phase | Description | Branch | Tag | Status | Notes |
| --- | --- | --- | --- | --- | --- |
| Phase 0 | Preflight and repo setup | `main` | N/A | Completed | Docker daemon active, Python 3.11, OpenJDK 17, Olist CSVs present |
| Phase 1 | Python + PostgreSQL ETL | `phase-1-etl` | `v0.1-phase1` | Completed | 5 tables loaded into staging; exact reconciliation; idempotent 2-run verified |
| Phase 2 | SQL data modelling | `phase-2-model` | `v0.2-phase2` | Not Started | |
| Phase 3 | Automation, tests, CI, Airflow | `phase-3-automation` | `v0.3-phase3` | Not Started | |
| Phase 4 | PySpark daily revenue | `phase-4-pyspark` | `v0.4-phase4` | Not Started | |
| Phase 5 | Kafka streaming | `phase-5-kafka` | `v0.5-phase5` | Not Started | |
| Phase 6 | Docker, dashboard, polish | `phase-6-polish` | `v1.0.0` | Not Started | |

## Measured Numbers (Phase 1)
- `staging.orders`: 99,441 rows in, 0 duplicates, 0 rejected, 99,441 loaded
- `staging.customers`: 99,441 rows in, 0 duplicates, 0 rejected, 99,441 loaded
- `staging.order_items`: 112,650 rows in, 0 duplicates, 0 rejected, 112,650 loaded
- `staging.products`: 32,951 rows in, 0 duplicates, 0 rejected, 32,951 loaded
- `staging.payments`: 103,886 rows in, 0 duplicates, 0 rejected, 103,886 loaded
- Full ETL pipeline elapsed time: 89.42s (run 1), 94.03s (verify run 1), 97.25s (verify run 2)
