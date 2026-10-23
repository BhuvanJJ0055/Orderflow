-- Ops tables for ETL state tracking, watermarking and run audit history

CREATE TABLE IF NOT EXISTS ops.etl_watermark (
    pipeline TEXT PRIMARY KEY,
    last_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    rows_in INT,
    rows_rejected INT,
    rows_loaded INT,
    error TEXT
);
