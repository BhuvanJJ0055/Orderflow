# Phase 2: SQL Data Modelling Verification

- **OS**: Windows 10 (10.0.26300)
- **CPU Count**: 12
- **Python Version**: 3.11.6 (tags/v3.11.6:8b6ee5b, Oct  2 2023, 14:57:12) [MSC v.1935 64 bit (AMD64)]

| Check | Result | Evidence |
| --- | --- | --- |
| Model script execution | PASS | Executed 01_model.sql successfully |
| Orphan and duplicate grain check | PASS | Orphan customers=0; Duplicate grain=0 |
| Exact independent revenue and item reconciliation | PASS | Fact items=112650, rev=13591643.70 \| Independent items=112650, rev=13591643.70 \| Staging gap: 0 items, 0.00 rev (0 excluded; full join satisfied) |
| Model idempotence on rerun | PASS | Run 1: items=112650, rev=13591643.70 \| Run 2: items=112650, rev=13591643.70 |
| Query execution plan and index verification | PASS | date_key=20171124 \| Default plan: Index/Bitmap Scan (fact_order_items) \| Index usable (seqscan=off): True (fact_order_items) |
| Analytical queries return results | PASS | Q1: 23 rows, Q2: 10 rows, Q3: 27 rows, Q4: 24 rows |
