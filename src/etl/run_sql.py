"""SQL script execution utility using raw psycopg2 connections.

Replaces psql across environments to ensure single-transaction execution
and non-zero exit on errors.
"""

import logging
import sys
from pathlib import Path

import psycopg2

from src.etl.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

logger = logging.getLogger(__name__)


def get_raw_connection() -> psycopg2.extensions.connection:
    """Create a raw psycopg2 connection using environment configuration."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def execute_sql_file(file_path: str | Path, conn: psycopg2.extensions.connection | None = None) -> None:
    """Execute a multi-statement SQL file in a single transaction.

    Rolls back and raises an exception on the first error.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    sql_text = path.read_text(encoding="utf-8")
    own_conn = conn is None
    db_conn = conn or get_raw_connection()

    try:
        with db_conn, db_conn.cursor() as cur:
            cur.execute(sql_text)
        logger.info("Successfully executed SQL script: %s", path)
    except (psycopg2.Error, OSError):
        logger.exception("Failed executing SQL script %s", path)
        if not db_conn.closed:
            db_conn.rollback()
        raise
    finally:
        if own_conn and not db_conn.closed:
            db_conn.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s]: %(message)s")
    if len(sys.argv) < 2:
        print("Usage: python -m src.etl.run_sql <path/to/file.sql>", file=sys.stderr)
        sys.exit(1)

    target_sql = sys.argv[1]
    try:
        execute_sql_file(target_sql)
        print(f"Executed: {target_sql}")
    except (psycopg2.Error, OSError, FileNotFoundError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
