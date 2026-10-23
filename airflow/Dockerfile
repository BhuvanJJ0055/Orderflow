FROM apache/airflow:2.10.3-python3.11

USER root
# Create dedicated virtual environment for project ETL dependencies
RUN python -m venv /opt/etl_venv \
    && chown -R airflow:root /opt/etl_venv

USER airflow
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN /opt/etl_venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
