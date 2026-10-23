"""Database loading and rejection quarantine storage module."""

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import Engine, create_engine

from src.etl.config import DATABASE_URL, REJECTS_DIR

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
    engine: Engine | None = None,
) -> int:
    """Load a DataFrame into PostgreSQL using multi-row insertion chunks."""
    eng = engine or get_engine()
    df.to_sql(
        name=table_name,
        schema=schema,
        con=eng,
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
