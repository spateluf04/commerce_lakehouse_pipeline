-- The 10 products with the most revenue on one day (:ds). Ties are broken by product_id, so the ranking is stable.
WITH product_day AS (
    SELECT
        order_date,
        product_id,
        product_name,
        category,
        SUM(quantity)   AS units,
        SUM(line_total) AS revenue
    FROM silver_order_lines
    WHERE order_date = to_date(:ds)
      AND order_status NOT IN ('cancelled', 'returned')
    GROUP BY order_date, product_id, product_name, category
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY revenue DESC, product_id) AS revenue_rank
    FROM product_day
)
SELECT
    order_date,
    revenue_rank,
    product_id,
    product_name,
    category,
    units,
    CAST(revenue AS DECIMAL(18, 2)) AS revenue
FROM ranked
WHERE revenue_rank <= 10
