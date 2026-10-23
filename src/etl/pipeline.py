"""Main ETL pipeline orchestrator supporting full and incremental modes."""

import argparse
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

import src.etl.watermark as watermark_module
from src.etl.config import BASE_DIR, LOGS_DIR, RAW_FILES
from src.etl.extract import extract_table
from src.etl.load import get_engine, load_table, record_run, save_rejects
from src.etl.run_sql import execute_sql_file
from src.etl.transform import clean_table, filter_incremental_orders
from src.etl.validate import apply_rules

logger = logging.getLogger("src.etl.pipeline")
PIPELINE_NAME = "ecom_batch_pipeline"


def setup_logging(logs_dir: Path = LOGS_DIR) -> None:
    """Configure dual logging to stdout and logs/etl.log."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "etl.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def run_pipeline(
    mode: str = "full",
    cutoff: str | None = None,
) -> dict[str, dict[str, int]]:
    """Execute batch pipeline in full or incremental mode."""
    setup_logging()
    start_time = time.time()
    started_at = datetime.now(UTC)
    logger.info("Starting pipeline '%s' in mode='%s' (cutoff=%s)", PIPELINE_NAME, mode, cutoff)

    # Test-only hook check: fail after specified table
    fail_after = os.getenv("ETL_FAIL_AFTER")

    engine = None
    try:
        engine = get_engine()
        # Ping database early to verify connectivity
        with engine.connect() as ping_conn:
            ping_conn.execute(text("SELECT 1;"))
    except Exception as db_err:
        logger.error("Database connection failed: %s", db_err)
        record_run(
            pipeline=PIPELINE_NAME,
            mode=mode,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            status="failed",
            rows_in=0,
            rows_rejected=0,
            rows_loaded=0,
            error=str(db_err),
            engine=None,
        )
        raise

    # Check watermark if in incremental mode
    current_watermark = None
    if mode == "incremental":
        current_watermark = watermark_module.get(engine, PIPELINE_NAME)
        if current_watermark is None:
            logger.info("No existing watermark found; falling back to full mode.")
            mode = "full"

    summary: dict[str, dict[str, int]] = {}
    total_in = 0
    total_rejected = 0
    total_loaded = 0

    try:
        # 1. Extract raw data
        raw_tables = {tbl: extract_table(tbl) for tbl in RAW_FILES}

        # 2. Extract existing order ids if incremental
        existing_order_ids: set[str] = set()
        if mode == "incremental":
            with engine.connect() as conn:
                existing_res = conn.execute(text("SELECT order_id FROM staging.orders;")).fetchall()
                existing_order_ids = {r[0] for r in existing_res}

        # 3. Clean and filter orders
        raw_orders = raw_tables["orders"]
        clean_orders_df, orders_dups = clean_table(raw_orders, "orders")

        # Apply cutoff / incremental watermark filtering to orders
        if mode == "incremental":
            orders_to_validate = filter_incremental_orders(
                clean_orders_df,
                watermark=current_watermark,
                existing_order_ids=existing_order_ids,
                cutoff=cutoff,
            )
        elif cutoff:
            orders_to_validate = filter_incremental_orders(
                clean_orders_df,
                watermark=None,
                cutoff=cutoff,
            )
        else:
            orders_to_validate = clean_orders_df

        valid_orders, rejected_orders = apply_rules(orders_to_validate, "orders")
        allowed_order_ids = set(valid_orders["order_id"])

        # 4. Clean, filter and validate other tables
        processed_data: dict[str, tuple[pd.DataFrame, pd.DataFrame, int]] = {}

        for tbl, raw_df in raw_tables.items():
            if tbl == "orders":
                save_rejects(rejected_orders, "orders")
                processed_data["orders"] = (valid_orders, rejected_orders, orders_dups)
                continue

            clean_df, dups = clean_table(raw_df, tbl)

            # Filter items and payments to match allowed orders when cutoff or incremental
            if (cutoff or mode == "incremental") and tbl in ["order_items", "payments"]:
                clean_df = clean_df[clean_df["order_id"].isin(allowed_order_ids)].copy()

            valid_df, rejected_df = apply_rules(clean_df, tbl)
            save_rejects(rejected_df, tbl)
            processed_data[tbl] = (valid_df, rejected_df, dups)

        # 5. Atomic database load inside a single transaction
        with engine.begin() as conn:
            for tbl in ["customers", "products", "orders", "order_items", "payments"]:
                valid_df, rejected_df, dups = processed_data[tbl]
                raw_in = len(raw_tables[tbl])
                if mode == "incremental" and tbl in ["orders", "order_items", "payments"] or cutoff and tbl in ["orders", "order_items", "payments"]:
                    raw_in = len(valid_df) + len(rejected_df) + dups

                if_exists_strat = "replace"
                if mode == "incremental" and tbl in ["orders", "order_items", "payments"]:
                    if_exists_strat = "append"

                loaded = 0
                if not valid_df.empty or if_exists_strat == "replace":
                    loaded = load_table(
                        valid_df,
                        tbl,
                        schema="staging",
                        if_exists=if_exists_strat,
                        connection_or_engine=conn,
                    )

                summary[tbl] = {
                    "rows_in": raw_in,
                    "duplicates": dups,
                    "rejected": len(rejected_df),
                    "loaded": loaded,
                }
                total_in += raw_in
                total_rejected += len(rejected_df)
                total_loaded += loaded

                # Test-only failure hook
                if fail_after and fail_after == tbl:
                    raise RuntimeError(f"ETL_FAIL_AFTER hook triggered after processing '{tbl}'")

            # Update watermark in the same transaction
            if not valid_orders.empty:
                max_ts = valid_orders["order_purchase_timestamp"].max()
                if pd.notna(max_ts):
                    watermark_module.set_(conn, PIPELINE_NAME, max_ts.to_pydatetime())
                    logger.info("Updated watermark to %s", max_ts)

        # 6. Rebuild analytics star schema model if incremental succeeded
        if mode == "incremental":
            model_path = BASE_DIR / "sql" / "model" / "01_model.sql"
            execute_sql_file(model_path)

        # 7. Record successful run audit
        finished_at = datetime.now(UTC)
        elapsed_seconds = round(time.time() - start_time, 2)
        record_run(
            pipeline=PIPELINE_NAME,
            mode=mode,
            started_at=started_at,
            finished_at=finished_at,
            status="success",
            rows_in=total_in,
            rows_rejected=total_rejected,
            rows_loaded=total_loaded,
            error=None,
            engine=engine,
        )

        logger.info("ETL pipeline run finished successfully in %.2f seconds", elapsed_seconds)

        # Write summary to logs/last_run_summary.json
        summary_path = LOGS_DIR / "last_run_summary.json"
        full_report: dict[str, Any] = {
            "mode": mode,
            "cutoff": cutoff,
            "elapsed_seconds": elapsed_seconds,
            "tables": summary,
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(full_report, f, indent=2)

        # Print summary table
        print("\n==================== ETL RUN SUMMARY ====================")
        print(f"Mode: {mode} | Cutoff: {cutoff} | Elapsed Time: {elapsed_seconds}s")
        print(f"{'Table':<15} {'Rows In':<10} {'Duplicates':<12} {'Rejected':<10} {'Loaded':<10}")
        print("-" * 57)
        for tbl, counts in summary.items():
            print(
                f"{tbl:<15} {counts['rows_in']:<10} {counts['duplicates']:<12} "
                f"{counts['rejected']:<10} {counts['loaded']:<10}"
            )
        print("=========================================================\n")

        return summary

    except Exception as err:
        logger.error("Pipeline run failed: %s", err)
        record_run(
            pipeline=PIPELINE_NAME,
            mode=mode,
            started_at=started_at,
            finished_at=datetime.now(UTC),
            status="failed",
            rows_in=total_in,
            rows_rejected=total_rejected,
            rows_loaded=total_loaded,
            error=str(err),
            engine=engine,
        )
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-Time E-Commerce ETL Pipeline")
    parser.add_argument("--mode", choices=["full", "incremental"], default="full", help="Pipeline execution mode")
    parser.add_argument("--cutoff", type=str, default=None, help="Cutoff timestamp YYYY-MM-DD for simulated arrival")
    args = parser.parse_args()

    try:
        run_pipeline(mode=args.mode, cutoff=args.cutoff)
    except Exception as e:  # noqa: BLE001
        print(f"PIPELINE FAILURE: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
