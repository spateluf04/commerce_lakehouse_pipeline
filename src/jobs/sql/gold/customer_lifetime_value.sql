-- Lifetime value of every customer with at least one order that counts as revenue.
-- Rebuilt in full from all of silver on every run, so re-running or backfilling any date gives
-- the same result. (At a much larger scale this would become an incremental MERGE.)
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.state,
    MIN(o.order_date)                          AS first_order_date,
    MAX(o.order_date)                          AS last_order_date,
    COUNT(*)                                   AS orders,
    CAST(SUM(o.order_total) AS DECIMAL(18, 2)) AS lifetime_revenue,
    CAST(AVG(o.order_total) AS DECIMAL(18, 2)) AS avg_order_value
FROM silver_orders AS o
JOIN silver_customers AS c
  ON o.customer_id = c.customer_id
WHERE o.status NOT IN ('cancelled', 'returned')
GROUP BY c.customer_id, c.first_name, c.last_name, c.state
