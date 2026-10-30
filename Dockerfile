# ETL pipeline image
# Build:  docker build -t orderflow-etl .
# Run:    docker run --env-file .env --network ecom_net orderflow-etl
FROM python:3.11-slim AS base

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements-extra.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir kafka-python-ng==2.2.3

COPY src/       src/
COPY sql/       sql/
COPY scripts/   scripts/
COPY streaming/ streaming/
COPY .env.example .env.example

ENV PYTHONPATH=/app

CMD ["python", "-m", "src.etl.pipeline"]
