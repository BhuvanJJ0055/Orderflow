"""Main ETL pipeline orchestrator."""

import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from src.etl.config import LOGS_DIR, RAW_FILES
from src.etl.extract import extract_table
from src.etl.load import get_engine, load_table, save_rejects
from src.etl.transform import clean_table
from src.etl.validate import apply_rules

logger = logging.getLogger("src.etl.pipeline")


def setup_logging(logs_dir: Path = LOGS_DIR) -> None:
    """Configure dual logging to stdout and logs/etl.log."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "etl.log"

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Avoid duplicate handlers on rerun
    root_logger.handlers.clear()

    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s]: %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)


def run_pipeline() -> dict[str, dict[str, int]]:
    """Execute end-to-end extraction, transformation, validation, and staging loading."""
    setup_logging()
    start_time = time.time()
    logger.info("Starting ETL batch pipeline run")

    engine = get_engine()
    summary: dict[str, dict[str, int]] = {}

    for table_name in RAW_FILES:
        logger.info("--- Processing table: %s ---", table_name)

        # 1. Extract
        raw_df = extract_table(table_name)
        rows_in = len(raw_df)

        # 2. Clean & Deduplicate
        clean_df, duplicates = clean_table(raw_df, table_name)

        # 3. Validate & Quarantine
        valid_df, rejected_df = apply_rules(clean_df, table_name)
        rejected = len(rejected_df)

        # 4. Save Rejects
        save_rejects(rejected_df, table_name)

        # 5. Load into Staging
        loaded = load_table(valid_df, table_name, schema="staging", if_exists="replace", engine=engine)

        # Reconcile counts: rows_in == duplicates + rejected + loaded
        assert rows_in == duplicates + rejected + loaded, (
            f"Row count mismatch for {table_name}: rows_in({rows_in}) != "
            f"duplicates({duplicates}) + rejected({rejected}) + loaded({loaded})"
        )

        summary[table_name] = {
            "rows_in": rows_in,
            "duplicates": duplicates,
            "rejected": rejected,
            "loaded": loaded,
        }
        logger.info(
            "Table %s reconciled: in=%d, duplicates=%d, rejected=%d, loaded=%d",
            table_name,
            rows_in,
            duplicates,
            rejected,
            loaded,
        )

    elapsed_seconds = round(time.time() - start_time, 2)
    logger.info("ETL pipeline run finished in %.2f seconds", elapsed_seconds)

    # Write summary to logs/last_run_summary.json
    summary_path = LOGS_DIR / "last_run_summary.json"
    full_report: dict[str, Any] = {
        "elapsed_seconds": elapsed_seconds,
        "tables": summary,
    }
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(full_report, f, indent=2)

    # Print visible summary block
    print("\n==================== ETL RUN SUMMARY ====================")
    print(f"Total Elapsed Time: {elapsed_seconds}s")
    print(f"{'Table':<15} {'Rows In':<10} {'Duplicates':<12} {'Rejected':<10} {'Loaded':<10}")
    print("-" * 57)
    for tbl, counts in summary.items():
        print(
            f"{tbl:<15} {counts['rows_in']:<10} {counts['duplicates']:<12} "
            f"{counts['rejected']:<10} {counts['loaded']:<10}"
        )
    print("=========================================================\n")

    return summary


if __name__ == "__main__":
    run_pipeline()
