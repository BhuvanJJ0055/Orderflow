-- Analytics Queries

-- 1. Monthly revenue and order count (delivered orders only)
SELECT
    d.year,
    d.month,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_revenue
FROM analytics.fact_order_items f
JOIN analytics.dim_date d ON f.date_key = d.date_key
WHERE f.order_status = 'delivered'
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2. Top 10 product categories by revenue
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_revenue
FROM analytics.fact_order_items f
JOIN analytics.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Revenue by customer state
SELECT
    c.state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.price) AS total_revenue
FROM analytics.fact_order_items f
JOIN analytics.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.state
ORDER BY total_revenue DESC;

-- 4. Monthly revenue with a running total using window function
WITH monthly_metrics AS (
    SELECT
        d.year,
        d.month,
        TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
        SUM(f.price) AS monthly_revenue
    FROM analytics.fact_order_items f
    JOIN analytics.dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, TO_CHAR(d.full_date, 'YYYY-MM')
)
SELECT
    year,
    month,
    year_month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (ORDER BY year, month) AS running_total_revenue
FROM monthly_metrics
ORDER BY year, month;
