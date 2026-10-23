"""Configuration and environment settings for the ETL pipeline."""

import os
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

# Load environment variables from .env file at repo root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

# PostgreSQL database configuration
POSTGRES_USER = os.getenv("POSTGRES_USER", "ecom")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecom")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# Encoded password avoids issues if password contains special characters
_encoded_password = quote_plus(POSTGRES_PASSWORD)
DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{_encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Directory paths
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
REJECTS_DIR = DATA_DIR / "rejects"
LOGS_DIR = BASE_DIR / "logs"

# Mapping of canonical table names to their CSV file names
RAW_FILES: dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
}
