-- Revenue, orders and units sold per product category for one day (:ds).
-- Revenue rule: cancelled and returned orders don't count as revenue (docs/standards.md).
SELECT
    order_date,
    category,
    COUNT(DISTINCT order_id)                AS orders,
    SUM(quantity)                           AS units,
    CAST(SUM(line_total) AS DECIMAL(18, 2)) AS revenue
FROM silver_order_lines
WHERE order_date = to_date(:ds)
  AND order_status NOT IN ('cancelled', 'returned')
GROUP BY order_date, category
