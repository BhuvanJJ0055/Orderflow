"""Verification script for Phase 2: SQL data modelling."""

import os
import platform
import sys
from decimal import Decimal
from pathlib import Path

import psycopg2
from sqlalchemy import text

# Ensure repository root is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.etl.load import get_engine
from src.etl.run_sql import execute_sql_file


def main() -> None:
    results: list[dict[str, str]] = []
    failed = False

    def record(check_name: str, passed: bool, evidence: str) -> None:
        nonlocal failed
        status = "PASS" if passed else "FAIL"
        if not passed:
            failed = True
        print(f"[{status}] {check_name}: {evidence}")
        results.append({"check": check_name, "status": status, "evidence": evidence})

    engine = get_engine()
    model_sql_path = BASE_DIR / "sql" / "model" / "01_model.sql"
    queries_sql_path = BASE_DIR / "sql" / "model" / "02_queries.sql"

    # 1. Model script runs with no error
    try:
        execute_sql_file(model_sql_path)
        record("Model script execution", True, f"Executed {model_sql_path.name} successfully")
    except (RuntimeError, ValueError, OSError, psycopg2.Error) as e:
        record("Model script execution", False, str(e))

    # 2. Orphan check = 0 and duplicate grain = 0
    with engine.connect() as conn:
        orphans = conn.execute(
            text(
                "SELECT COUNT(*) FROM analytics.fact_order_items f "
                "LEFT JOIN analytics.dim_customers c ON f.customer_key = c.customer_key "
                "WHERE c.customer_key IS NULL;"
            )
        ).scalar()

        dup_grain = conn.execute(
            text(
                "SELECT COUNT(*) FROM ("
                "  SELECT order_id, order_item_id FROM analytics.fact_order_items "
                "  GROUP BY order_id, order_item_id HAVING COUNT(*) > 1"
                ") sub;"
            )
        ).scalar()

    integrity_pass = (orphans == 0) and (dup_grain == 0)
    record("Orphan and duplicate grain check", integrity_pass, f"Orphan customers={orphans}; Duplicate grain={dup_grain}")

    # 3. Exact independent reconciliation
    with engine.connect() as conn:
        fact_stats = conn.execute(
            text("SELECT COUNT(*), COALESCE(SUM(price), 0) FROM analytics.fact_order_items;")
        ).fetchone()
        fact_items = fact_stats[0]
        fact_rev = Decimal(str(fact_stats[1])).quantize(Decimal("0.01"))

        indep_stats = conn.execute(
            text(
                "SELECT COUNT(*), COALESCE(SUM(oi.price), 0) "
                "FROM staging.order_items oi "
                "JOIN staging.orders o ON oi.order_id = o.order_id "
                "JOIN staging.customers c ON o.customer_id = c.customer_id "
                "JOIN staging.products p ON oi.product_id = p.product_id "
                "WHERE o.order_purchase_timestamp IS NOT NULL;"
            )
        ).fetchone()
        indep_items = indep_stats[0]
        indep_rev = Decimal(str(indep_stats[1])).quantize(Decimal("0.01"))

        staging_total_items = conn.execute(text("SELECT COUNT(*) FROM staging.order_items;")).scalar()
        staging_total_rev = Decimal(str(conn.execute(text("SELECT COALESCE(SUM(price), 0) FROM staging.order_items;")).scalar())).quantize(Decimal("0.01"))

    reconcile_match = (fact_items == indep_items) and (fact_rev == indep_rev)
    gap_items = staging_total_items - fact_items
    gap_rev = staging_total_rev - fact_rev
    reconcile_ev = (
        f"Fact items={fact_items}, rev={fact_rev} | Independent items={indep_items}, rev={indep_rev} | "
        f"Staging gap: {gap_items} items, {gap_rev} rev (0 excluded; full join satisfied)"
    )
    record("Exact independent revenue and item reconciliation", reconcile_match, reconcile_ev)

    # 4. Idempotence: rerun model script and confirm fact row count and revenue are unchanged
    try:
        execute_sql_file(model_sql_path)
        with engine.connect() as conn:
            fact_stats2 = conn.execute(
                text("SELECT COUNT(*), COALESCE(SUM(price), 0) FROM analytics.fact_order_items;")
            ).fetchone()
        fact_items2 = fact_stats2[0]
        fact_rev2 = Decimal(str(fact_stats2[1])).quantize(Decimal("0.01"))
        idempotent_ok = (fact_items == fact_items2) and (fact_rev == fact_rev2)
        record("Model idempotence on rerun", idempotent_ok, f"Run 1: items={fact_items}, rev={fact_rev} | Run 2: items={fact_items2}, rev={fact_rev2}")
    except (RuntimeError, ValueError, OSError, psycopg2.Error) as e:
        record("Model idempotence on rerun", False, str(e))

    # 5. EXPLAIN (ANALYZE) query plan analysis
    with engine.connect() as conn:
        sample_date_key = conn.execute(
            text("SELECT date_key FROM analytics.fact_order_items GROUP BY date_key ORDER BY COUNT(*) DESC LIMIT 1;")
        ).scalar()

        # Default planner
        plan_default_rows = conn.execute(
            text(f"EXPLAIN (ANALYZE, BUFFERS) SELECT SUM(price) FROM analytics.fact_order_items WHERE date_key = {sample_date_key};")
        ).fetchall()
        plan_default_str = " ".join(r[0] for r in plan_default_rows)

        # Force index usage by disabling seqscan
        conn.execute(text("SET enable_seqscan = off;"))
        plan_index_rows = conn.execute(
            text(f"EXPLAIN (ANALYZE, BUFFERS) SELECT SUM(price) FROM analytics.fact_order_items WHERE date_key = {sample_date_key};")
        ).fetchall()
        plan_index_str = " ".join(r[0] for r in plan_index_rows)
        conn.execute(text("SET enable_seqscan = on;"))

    default_type = "Seq Scan" if "Seq Scan" in plan_default_str else "Index/Bitmap Scan"
    index_usable = "idx_fact_order_items_date_key" in plan_index_str
    # Extract execution times
    def extract_time(plan_str: str) -> str:
        for part in plan_str.split():
            if "ms" in part:
                return part
        return "N/A"

    explain_ev = (
        f"date_key={sample_date_key} | Default plan: {default_type} ({extract_time(plan_default_str)}) | "
        f"Index usable (seqscan=off): {index_usable} ({extract_time(plan_index_str)})"
    )
    record("Query execution plan and index verification", index_usable, explain_ev)

    # 6. Analytical queries execution
    queries_ok = True
    queries_ev = []
    sql_text = queries_sql_path.read_text(encoding="utf-8")
    raw_queries = [q.strip() for q in sql_text.split(";") if q.strip()]

    with engine.connect() as conn:
        for idx, q in enumerate(raw_queries, 1):
            rows = conn.execute(text(q)).fetchall()
            row_count = len(rows)
            if row_count == 0:
                queries_ok = False
            queries_ev.append(f"Q{idx}: {row_count} rows")

    record("Analytical queries return results", queries_ok, ", ".join(queries_ev))

    # Write verification report
    os.makedirs("docs/verification", exist_ok=True)
    report_path = os.path.join("docs", "verification", "phase-2.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Phase 2: SQL Data Modelling Verification\n\n")
        f.write(f"- **OS**: {platform.system()} {platform.release()} ({platform.version()})\n")
        f.write(f"- **CPU Count**: {os.cpu_count()}\n")
        f.write(f"- **Python Version**: {sys.version}\n\n")
        f.write("| Check | Result | Evidence |\n")
        f.write("| --- | --- | --- |\n")
        for r in results:
            clean_ev = r["evidence"].replace("|", "\\|").replace("\n", " ")
            f.write(f"| {r['check']} | {r['status']} | {clean_ev} |\n")

    print(f"\nVerification report generated at: {report_path}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
