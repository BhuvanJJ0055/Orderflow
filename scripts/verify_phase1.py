"""Verification script for Phase 1: Python + PostgreSQL ETL."""

import os
import platform
import subprocess
import sys
from pathlib import Path

# Ensure repository root is on sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd
from sqlalchemy import text

from src.etl.config import REJECTS_DIR
from src.etl.load import get_engine
from src.etl.pipeline import run_pipeline
from src.etl.validate import VALID_ORDER_STATUSES


def run_command(cmd: list[str]) -> tuple[int, str, str]:
    """Execute a shell command and return returncode, stdout, stderr."""
    try:
        res = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=30)
        return res.returncode, res.stdout.strip(), res.stderr.strip()
    except (subprocess.SubprocessError, OSError) as e:
        return -1, "", str(e)


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

    # 1. Container health and schemas
    _code, out, _ = run_command(["docker", "compose", "ps", "postgres"])
    is_healthy = "healthy" in out.lower()

    with engine.connect() as conn:
        schema_rows = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('staging', 'analytics', 'ops');")
        ).fetchall()
        found_schemas = {r[0] for r in schema_rows}
    schemas_ok = found_schemas == {"staging", "analytics", "ops"}
    check1_pass = is_healthy and schemas_ok
    record("Container healthy and schemas exist", check1_pass, f"Postgres healthy={is_healthy}; Found schemas={sorted(found_schemas)}")

    # 2 & 3. Run pipeline (run 1) and verify reconciliation
    print("\n>>> Running Pipeline (Run 1)...")
    summary1: dict[str, dict[str, int]] = {}
    try:
        summary1 = run_pipeline()
        pipeline1_ok = len(summary1) == 5
        record("Pipeline run 1 exits 0 with summary", pipeline1_ok, f"Processed {len(summary1)} tables")
    except (RuntimeError, ValueError, AssertionError) as e:
        record("Pipeline run 1 exits 0 with summary", False, str(e))

    reconcile_ok = True
    reconcile_ev = []
    for tbl, counts in summary1.items():
        rin = counts["rows_in"]
        d = counts["duplicates"]
        r = counts["rejected"]
        loaded = counts["loaded"]
        calc = d + r + loaded
        if rin != calc:
            reconcile_ok = False
        reconcile_ev.append(f"{tbl}: {rin} == {d}+{r}+{loaded}")
    record("Reconciliation rows_in == duplicates + rejected + loaded", reconcile_ok, "; ".join(reconcile_ev))

    # 4. Five tables in staging and DB count equals loaded count
    db_counts_ok = True
    db_counts_ev = []
    with engine.connect() as conn:
        for tbl in ["orders", "customers", "order_items", "products", "payments"]:
            count_res = conn.execute(text(f"SELECT COUNT(*) FROM staging.{tbl};")).scalar()
            expected = summary1.get(tbl, {}).get("loaded", -1)
            if count_res != expected:
                db_counts_ok = False
            db_counts_ev.append(f"staging.{tbl}: DB={count_res}, loaded={expected}")
    record("Staging tables exist with matching row counts", db_counts_ok, "; ".join(db_counts_ev))

    # 5. SELECT DISTINCT order_status returns only lowercase valid statuses
    with engine.connect() as conn:
        statuses = [r[0] for r in conn.execute(text("SELECT DISTINCT order_status FROM staging.orders;")).fetchall()]
    status_ok = all(s in VALID_ORDER_STATUSES and s == s.lower() for s in statuses)
    record("Order status normalization", status_ok, f"Statuses: {sorted(statuses)}")

    # 6. Second run gives identical counts (idempotence)
    print("\n>>> Running Pipeline (Run 2) for Idempotence...")
    try:
        summary2 = run_pipeline()
        idempotent_ok = summary1 == summary2
        record("Second run is idempotent", idempotent_ok, f"Run 1 matches Run 2 exactly: {idempotent_ok}")
    except (RuntimeError, ValueError, AssertionError) as e:
        record("Second run is idempotent", False, str(e))

    # 7. Reject files check
    reject_check_ok = True
    reject_ev = []
    for tbl, counts in summary1.items():
        rej_count = counts["rejected"]
        rej_file = REJECTS_DIR / f"{tbl}_rejected.csv"
        if rej_count > 0:
            if rej_file.exists():
                df_rej = pd.read_csv(rej_file, nrows=5)
                has_reason = "reject_reason" in df_rej.columns
                if not has_reason:
                    reject_check_ok = False
                reject_ev.append(f"{tbl}: file exists with {rej_count} rows, has reject_reason={has_reason}")
            else:
                reject_check_ok = False
                reject_ev.append(f"{tbl}: rejected={rej_count} but reject file missing")
        else:
            reject_ev.append(f"{tbl}: rejected=0 (no reject file needed)")
    record("Quarantine reject files integrity", reject_check_ok, "; ".join(reject_ev))

    # Write verification report
    os.makedirs("docs/verification", exist_ok=True)
    report_path = os.path.join("docs", "verification", "phase-1.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Phase 1: Python + PostgreSQL ETL Verification\n\n")
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
