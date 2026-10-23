"""CSV extraction module for raw datasets."""

import logging
from pathlib import Path

import pandas as pd

from src.etl.config import RAW_DIR, RAW_FILES

logger = logging.getLogger(__name__)


def extract_table(table_name: str, raw_dir: Path = RAW_DIR) -> pd.DataFrame:
    """Read a specific table CSV and return a raw DataFrame.

    Raises FileNotFoundError with a clear path message if the CSV does not exist.
    """
    if table_name not in RAW_FILES:
        raise ValueError(f"Unknown table '{table_name}'. Expected one of {list(RAW_FILES.keys())}")

    filename = RAW_FILES[table_name]
    file_path = raw_dir / filename

    if not file_path.exists():
        raise FileNotFoundError(f"Missing raw CSV file for table '{table_name}': {file_path}")

    # Read CSV with default string conversion where appropriate
    df = pd.read_csv(file_path)
    logger.info("Extracted %s: %d rows, %d columns from %s", table_name, len(df), len(df.columns), file_path.name)
    return df


def extract_all(raw_dir: Path = RAW_DIR) -> dict[str, pd.DataFrame]:
    """Extract all five raw CSV datasets."""
    datasets: dict[str, pd.DataFrame] = {}
    for table_name in RAW_FILES:
        datasets[table_name] = extract_table(table_name, raw_dir)
    return datasets
