-- Analytics Star Schema: Dimensions and Order Items Fact Table

-- 1. Customer Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_unique_id TEXT UNIQUE NOT NULL,
    city TEXT,
    state TEXT
);

INSERT INTO analytics.dim_customers (customer_unique_id, city, state)
SELECT DISTINCT ON (customer_unique_id)
    customer_unique_id,
    customer_city,
    customer_state
FROM staging.customers
WHERE customer_unique_id IS NOT NULL
ORDER BY customer_unique_id, customer_id
ON CONFLICT (customer_unique_id) DO NOTHING;

-- 2. Product Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id TEXT UNIQUE NOT NULL,
    category TEXT,
    weight_g NUMERIC
);

INSERT INTO analytics.dim_products (product_id, category, weight_g)
SELECT DISTINCT ON (product_id)
    product_id,
    product_category_name,
    product_weight_g
FROM staging.products
WHERE product_id IS NOT NULL
ORDER BY product_id
ON CONFLICT (product_id) DO NOTHING;

-- 3. Date Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

INSERT INTO analytics.dim_date (date_key, full_date, year, quarter, month, day, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    TRIM(TO_CHAR(d, 'Day')) AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2016-01-01'::DATE, '2019-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (date_key) DO NOTHING;

-- 4. Order Items Fact Table
CREATE TABLE IF NOT EXISTS analytics.fact_order_items (
    order_id TEXT NOT NULL,
    order_item_id INT NOT NULL,
    customer_key INT NOT NULL REFERENCES analytics.dim_customers(customer_key),
    product_key INT NOT NULL REFERENCES analytics.dim_products(product_key),
    date_key INT NOT NULL REFERENCES analytics.dim_date(date_key),
    order_status TEXT NOT NULL,
    purchase_ts TIMESTAMP NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    freight_value NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

INSERT INTO analytics.fact_order_items (
    order_id,
    order_item_id,
    customer_key,
    product_key,
    date_key,
    order_status,
    purchase_ts,
    price,
    freight_value
)
SELECT
    oi.order_id,
    oi.order_item_id,
    dc.customer_key,
    dp.product_key,
    TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INT AS date_key,
    o.order_status,
    o.order_purchase_timestamp AS purchase_ts,
    oi.price::NUMERIC(10,2),
    oi.freight_value::NUMERIC(10,2)
FROM staging.order_items oi
JOIN staging.orders o ON oi.order_id = o.order_id
JOIN staging.customers c ON o.customer_id = c.customer_id
JOIN analytics.dim_customers dc ON c.customer_unique_id = dc.customer_unique_id
JOIN analytics.dim_products dp ON oi.product_id = dp.product_id
WHERE o.order_purchase_timestamp IS NOT NULL
ON CONFLICT (order_id, order_item_id) DO NOTHING;

-- 5. Foreign Key Indexes on Fact Table
CREATE INDEX IF NOT EXISTS idx_fact_order_items_customer_key ON analytics.fact_order_items(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_key ON analytics.fact_order_items(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_items_date_key ON analytics.fact_order_items(date_key);
