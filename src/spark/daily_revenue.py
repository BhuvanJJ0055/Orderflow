import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from sqlalchemy import create_engine, text

# Load environment variables from .env at repository root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

# On Windows, configure HADOOP_HOME if winutils is present
local_hadoop = BASE_DIR / "hadoop"
if os.name == "nt" and (local_hadoop / "bin" / "winutils.exe").exists():
    os.environ.setdefault("HADOOP_HOME", str(local_hadoop.resolve()))
    bin_dir = str((local_hadoop / "bin").resolve())
    if bin_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{bin_dir};{os.environ.get('PATH', '')}"


def get_db_credentials() -> dict[str, str]:
    """Retrieve database connection settings from environment variables."""
    return {
        "user": os.getenv("POSTGRES_USER", "ecom"),
        "password": os.getenv("POSTGRES_PASSWORD", "change_me"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "ecom"),
    }


def ensure_target_table_exists(creds: dict[str, str]) -> None:
    """Ensure target table exists so truncate=true works idempotently."""
    db_url = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS analytics.agg_daily_category_revenue (
                order_date DATE,
                category TEXT,
                revenue NUMERIC(10, 2),
                orders BIGINT
            );
            """
        ))
    engine.dispose()


def run_daily_revenue_job() -> int:
    """Run PySpark aggregation of daily category revenue from star schema fact table."""
    creds = get_db_credentials()
    ensure_target_table_exists(creds)

    jdbc_url = f"jdbc:postgresql://{creds['host']}:{creds['port']}/{creds['database']}"

    spark = (
        SparkSession.builder
        .appName("DailyRevenueByCategory")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    try:
        # Read fact table and product dimension over JDBC
        fact_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "analytics.fact_order_items")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        dim_prod_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "analytics.dim_products")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        # Join fact with dim_products on product_key
        joined = fact_df.join(dim_prod_df, on="product_key", how="inner")

        # Derive order_date from purchase_ts
        with_date = joined.withColumn("order_date", F.to_date("purchase_ts"))

        # Group by order_date and category, aggregate revenue and unique order count
        agg_df = (
            with_date.groupBy("order_date", "category")
            .agg(
                F.round(F.sum("price"), 2).alias("revenue"),
                F.countDistinct("order_id").alias("orders"),
            )
        )

        # Write to analytics.agg_daily_category_revenue with truncate=true
        (
            agg_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "analytics.agg_daily_category_revenue")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

        count = agg_df.count()
        print(f"Wrote {count} rows to analytics.agg_daily_category_revenue")
        return count
    finally:
        spark.stop()


def main() -> None:
    count = run_daily_revenue_job()
    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
