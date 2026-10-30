# Orderflow — Real-Time E-Commerce Data Pipeline

End-to-end data engineering reference project built in six phases:

| Phase | What it does |
|-------|-------------|
| 0 | Preflight, repo scaffold, sample-data generator |
| 1 | Python + PostgreSQL ETL (extract → clean → validate → load) |
| 2 | SQL star schema, window functions, revenue reconciliation |
| 3 | Watermark tracking, incremental mode, crash-rollback, Airflow DAG, CI |
| 4 | PySpark daily-revenue aggregation over JDBC |
| 5 | Kafka streaming — idempotent consumer with manual offset commits |
| 6 | Docker images, Streamlit dashboard, comprehensive docs |

---

## Tech stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Database | PostgreSQL 16 |
| DataFrame | Pandas ≥ 2.2 |
| ORM / SQL | SQLAlchemy 2 |
| Batch processing | PySpark 3.x (local mode) |
| Streaming | Apache Kafka 3.8.1 (KRaft) |
| Orchestration | Apache Airflow 2.10.x |
| Dashboard | Streamlit 1.40 + Plotly 5 |
| Containerisation | Docker / Docker Compose |
| Testing | pytest ≥ 8 |
| Linting | Ruff |
| CI | GitHub Actions |

---

## Quick start (< 5 minutes)

### Prerequisites

- Docker Desktop ≥ 4.x  
- Python 3.11  
- Git

### 1 — Clone and configure

```bash
git clone <your-repo-url> orderflow
cd orderflow
cp .env.example .env          # edit POSTGRES_PASSWORD at minimum
```

### 2 — Start infrastructure

```bash
docker compose up -d postgres kafka
```

Wait for the postgres healthcheck to go green (≈ 10 s):

```bash
docker compose ps
```

### 3 — Set up Python environment

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt -r requirements-extra.txt
```

### 4 — Generate sample data

```bash
python scripts/make_sample_data.py
```

Writes ~112,000 rows across five CSV files into `data/raw/`.

### 5 — Run the ETL pipeline

```bash
python -m src.etl.pipeline
```

Logs a JSON summary to `logs/` and prints row counts.

### 6 — Build the star schema

```bash
python -m src.etl.run_sql sql/model
```

Creates `dim_customers`, `dim_products`, `dim_date`, and `fact_order_items`
in the `analytics` schema.

### 7 — Run the Streamlit dashboard

```bash
streamlit run dashboard/app.py
```

Open <http://localhost:8501>.

---

## Docker usage

### ETL one-shot (mount data volume)

```bash
docker compose --profile etl run --rm etl
```

### Dashboard

```bash
docker compose --profile dashboard up -d dashboard
```

Open <http://localhost:8501>.

### Full stack

```bash
docker compose --profile etl --profile dashboard up -d
```

---

## PySpark aggregation

```bash
# Requires Java 11+ on PATH and SPARK_HOME configured
python src/spark/daily_revenue.py
```

Reads `fact_order_items` + `dim_products` over JDBC and writes
`analytics.agg_daily_category_revenue` (≈ 18,990 rows).

---

## Kafka streaming

```bash
# 1. Create the topic
python scripts/create_topic.py

# 2. Start consumer in one terminal
python streaming/consumer.py

# 3. Produce events in another terminal
python streaming/producer.py --count 500
```

---

## Airflow DAG

```bash
cd airflow
docker compose up -d          # starts Airflow 2.10
# UI at http://localhost:8080  (airflow / airflow)
```

The DAG `ecom_pipeline_dag` runs the full pipeline daily at 02:00 UTC.

---

## Testing

```bash
pytest tests/ -v
```

All tests are pure-unit (no network / no Docker required).

---

## Linting

```bash
ruff check src/ streaming/ scripts/ dashboard/ tests/
```

---

## Directory layout

```
orderflow/
├── Dockerfile               # ETL image
├── docker-compose.yml       # postgres + kafka + etl + dashboard services
├── .env.example             # environment variable template
├── requirements.txt         # core deps
├── requirements-extra.txt   # dashboard + streaming deps
│
├── src/
│   ├── etl/                 # extract / transform / validate / load
│   │   ├── config.py
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── validate.py
│   │   ├── load.py
│   │   ├── watermark.py
│   │   ├── run_sql.py
│   │   └── pipeline.py      # entry-point: python -m src.etl.pipeline
│   └── spark/
│       └── daily_revenue.py # PySpark JDBC aggregation
│
├── streaming/
│   ├── producer.py          # Kafka event producer
│   └── consumer.py          # idempotent consumer (manual offset commits)
│
├── dags/
│   └── ecom_pipeline_dag.py  # Airflow DAG
│
├── sql/
│   ├── init/                # DDL run by postgres container on first start
│   └── model/               # star schema + analytics views
│
├── scripts/
│   ├── make_sample_data.py  # generate synthetic Olist-style CSVs
│   ├── create_topic.py      # Kafka topic setup
│   └── verify_phase*.py     # per-phase verification scripts
│
├── dashboard/
│   ├── Dockerfile           # Streamlit image
│   └── app.py               # multi-section analytics dashboard
│
├── tests/                   # pytest unit tests
├── docs/
│   ├── PROGRESS.md
│   ├── RESULTS.md
│   └── verification/        # per-phase verification reports
│
└── .github/
    └── workflows/
        └── ci.yml           # lint + test on every push
```

---

## Verification results

See [`docs/verification/SUMMARY.md`](docs/verification/SUMMARY.md) for the
complete, phase-by-phase verification table with measured numbers.

See [`docs/RESULTS.md`](docs/RESULTS.md) for headline metrics.

---

## Licence

MIT — see [LICENSE](LICENSE).
