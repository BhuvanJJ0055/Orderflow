"""ETL watermark persistence and retrieval module."""

from datetime import datetime

from sqlalchemy import Connection, Engine, text


def get(engine_or_conn: Engine | Connection, pipeline: str = "ecom_batch_pipeline") -> datetime | None:
    """Retrieve the latest processed timestamp for a pipeline, or None if unset."""
    query = text("SELECT last_ts FROM ops.etl_watermark WHERE pipeline = :pipeline;")
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.connect() as conn:
            result = conn.execute(query, {"pipeline": pipeline}).scalar()
    else:
        result = engine_or_conn.execute(query, {"pipeline": pipeline}).scalar()
    return result


def set_(
    engine_or_conn: Engine | Connection,
    pipeline: str,
    ts: datetime,
) -> None:
    """Upsert the watermark timestamp for a pipeline."""
    upsert_query = text(
        "INSERT INTO ops.etl_watermark (pipeline, last_ts) "
        "VALUES (:pipeline, :last_ts) "
        "ON CONFLICT (pipeline) DO UPDATE SET last_ts = EXCLUDED.last_ts;"
    )
    params = {"pipeline": pipeline, "last_ts": ts}
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            conn.execute(upsert_query, params)
    else:
        engine_or_conn.execute(upsert_query, params)
