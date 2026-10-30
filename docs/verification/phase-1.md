# Phase 1: Python + PostgreSQL ETL Verification

- **OS**: Windows 10 (10.0.26300)
- **CPU Count**: 12
- **Python Version**: 3.11.6 (tags/v3.11.6:8b6ee5b, Oct  2 2023, 14:57:12) [MSC v.1935 64 bit (AMD64)]

| Check | Result | Evidence |
| --- | --- | --- |
| Container healthy and schemas exist | PASS | Postgres healthy=True; Found schemas=['analytics', 'ops', 'staging'] |
| Pipeline run 1 exits 0 with summary | PASS | Processed 5 tables |
| Reconciliation rows_in == duplicates + rejected + loaded | PASS | orders: 99441 == 0+0+99441; customers: 99441 == 0+0+99441; products: 32951 == 0+0+32951; order_items: 112650 == 0+0+112650; payments: 103886 == 0+0+103886 |
| Staging tables exist with matching row counts | PASS | staging.orders: DB=99441, loaded=99441; staging.customers: DB=99441, loaded=99441; staging.order_items: DB=112650, loaded=112650; staging.products: DB=32951, loaded=32951; staging.payments: DB=103886, loaded=103886 |
| Order status normalization | PASS | Statuses: ['approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable'] |
| Second run is idempotent | PASS | Run 1 matches Run 2 exactly: True |
| Quarantine reject files integrity | PASS | orders: rejected=0 (no reject file needed); customers: rejected=0 (no reject file needed); products: rejected=0 (no reject file needed); order_items: rejected=0 (no reject file needed); payments: rejected=0 (no reject file needed) |
