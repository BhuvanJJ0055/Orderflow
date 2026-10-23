"""Database loading, rejection quarantine storage, and run audit tracking."""

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from sqlalchemy import Connection, Engine, create_engine, text

from src.etl.config import DATABASE_URL, LOGS_DIR, REJECTS_DIR

logger = logging.getLogger(__name__)


def get_engine(db_url: str | None = None) -> Engine:
    """Create a SQLAlchemy engine with connection pool pre-pinging."""
    url = db_url or DATABASE_URL
    return create_engine(url, pool_pre_ping=True)


def load_table(
    df: pd.DataFrame,
    table_name: str,
    schema: str = "staging",
    if_exists: str = "replace",
    connection_or_engine: Engine | Connection | None = None,
) -> int:
    """Load a DataFrame into PostgreSQL using multi-row insertion chunks."""
    target = connection_or_engine or get_engine()
    df.to_sql(
        name=table_name,
        schema=schema,
        con=target,
        if_exists=if_exists,
        index=False,
        chunksize=5000,
        method="multi",
    )
    logger.info("Loaded %d rows into %s.%s (if_exists=%s)", len(df), schema, table_name, if_exists)
    return len(df)


def save_rejects(
    rejected_df: pd.DataFrame,
    table_name: str,
    rejects_dir: Path = REJECTS_DIR,
) -> Path | None:
    """Write rejected records with reject_reason to CSV if any exist."""
    rejects_dir.mkdir(parents=True, exist_ok=True)
    reject_path = rejects_dir / f"{table_name}_rejected.csv"

    if len(rejected_df) > 0:
        rejected_df.to_csv(reject_path, index=False)
        logger.info("Saved %d rejected rows for '%s' to %s", len(rejected_df), table_name, reject_path)
        return reject_path

    # Clean up stale reject file if previous runs had errors but current run has none
    if reject_path.exists():
        reject_path.unlink()
    return None


def record_run(
    pipeline: str,
    mode: str,
    started_at: datetime,
    finished_at: datetime | None,
    status: str,
    rows_in: int,
    rows_rejected: int,
    rows_loaded: int,
    error: str | None = None,
    engine: Engine | None = None,
) -> None:
    """Record pipeline execution in ops.pipeline_runs or logs/failed_runs.jsonl if DB fails."""
    insert_sql = text(
        "INSERT INTO ops.pipeline_runs (pipeline, mode, started_at, finished_at, status, rows_in, rows_rejected, rows_loaded, error) "
        "VALUES (:pipeline, :mode, :started_at, :finished_at, :status, :rows_in, :rows_rejected, :rows_loaded, :error);"
    )
    params = {
        "pipeline": pipeline,
        "mode": mode,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": status,
        "rows_in": rows_in,
        "rows_rejected": rows_rejected,
        "rows_loaded": rows_loaded,
        "error": error,
    }

    try:
        eng = engine or get_engine()
        with eng.begin() as conn:
            conn.execute(insert_sql, params)
    except (psycopg2.Error, OSError, Exception) as exc:  # noqa: BLE001
        logger.warning("Could not write to ops.pipeline_runs (%s). Logging to failed_runs.jsonl", exc)
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        fallback_file = LOGS_DIR / "failed_runs.jsonl"
        log_payload = {
            "pipeline": pipeline,
            "mode": mode,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat() if finished_at else None,
            "status": status,
            "rows_in": rows_in,
            "rows_rejected": rows_rejected,
            "rows_loaded": rows_loaded,
            "error": error or str(exc),
        }
        with open(fallback_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_payload) + "\n")
