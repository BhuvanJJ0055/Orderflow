"""Airflow DAG orchestrating the batch e-commerce ETL and SQL data modeling.

Note: PySpark daily aggregation is not part of this DAG because the base
Airflow image does not bundle Java/Spark runtime libraries.
"""

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecom_batch_pipeline",
    default_args=default_args,
    description="Batch Olist ETL with data quality checks and star schema modeling",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "etl"],
) as dag:
    # 1. Incremental extract, clean, validate, quarantine and load into staging
    extract_clean_load = BashOperator(
        task_id="extract_clean_load",
        bash_command=(
            "cd /opt/airflow/project && "
            "/opt/etl_venv/bin/python -m src.etl.pipeline --mode incremental"
        ),
    )

    # 2. Rebuild star schema dimensions and fact table
    build_model = BashOperator(
        task_id="build_model",
        bash_command=(
            "cd /opt/airflow/project && "
            "/opt/etl_venv/bin/python -m src.etl.run_sql sql/model/01_model.sql"
        ),
    )

    # 3. Execute integrity and revenue reconciliation checks
    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command=(
            "cd /opt/airflow/project && "
            "/opt/etl_venv/bin/python -m src.etl.run_sql sql/model/03_checks.sql"
        ),
    )

    extract_clean_load >> build_model >> quality_checks
