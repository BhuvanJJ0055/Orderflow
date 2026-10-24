-- Table for real-time Kafka streamed orders
CREATE TABLE IF NOT EXISTS analytics.live_orders (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL,
    ingested_ts TIMESTAMPTZ DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_live_orders_event_ts ON analytics.live_orders (event_ts);
CREATE INDEX IF NOT EXISTS idx_live_orders_ingested_ts ON analytics.live_orders (ingested_ts);
