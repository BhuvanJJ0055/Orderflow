-- Integrity Checks for Analytics Star Schema

-- 1. Orphan customer keys in fact table (expect 0)
SELECT COUNT(*) AS orphan_customer_keys
FROM analytics.fact_order_items f
LEFT JOIN analytics.dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- 2. Duplicate grain on (order_id, order_item_id) (expect 0)
SELECT COUNT(*) AS duplicate_grain_count
FROM (
    SELECT order_id, order_item_id, COUNT(*) AS cnt
    FROM analytics.fact_order_items
    GROUP BY order_id, order_item_id
    HAVING COUNT(*) > 1
) sub;

-- 3. Staging revenue vs fact revenue
SELECT
    (SELECT SUM(price) FROM staging.order_items) AS staging_raw_revenue,
    (SELECT SUM(price) FROM analytics.fact_order_items) AS fact_revenue,
    (SELECT SUM(price) FROM staging.order_items) - (SELECT SUM(price) FROM analytics.fact_order_items) AS revenue_gap;

-- 4. Staging item count vs fact item count
SELECT
    (SELECT COUNT(*) FROM staging.order_items) AS staging_raw_items,
    (SELECT COUNT(*) FROM analytics.fact_order_items) AS fact_items,
    (SELECT COUNT(*) FROM staging.order_items) - (SELECT COUNT(*) FROM analytics.fact_order_items) AS items_gap;

-- 5. Independent reconciliation query
SELECT
    COUNT(*) AS independent_matched_items,
    SUM(oi.price) AS independent_matched_revenue
FROM staging.order_items oi
JOIN staging.orders o ON oi.order_id = o.order_id
JOIN staging.customers c ON o.customer_id = c.customer_id
JOIN staging.products p ON oi.product_id = p.product_id
WHERE o.order_purchase_timestamp IS NOT NULL;
