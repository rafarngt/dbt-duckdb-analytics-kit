-- Análisis de rendimiento de productos
-- Este análisis evalúa el rendimiento de los productos basado en ventas,
-- frecuencia de compra y contribución a los ingresos totales

WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_name,
        p.category as product_category,
        p.price as product_price,
        SUM(oi.quantity) AS total_units_sold,
        COUNT(DISTINCT o.order_id) AS order_count,
        COUNT(DISTINCT o.customer_id) AS customer_count,
        SUM(p.price * oi.quantity) AS total_revenue
    FROM {{ ref('dim_products') }} p
    JOIN {{ ref('stg_order_items') }} oi ON p.product_id = oi.product_id
    JOIN {{ ref('fct_orders') }} o ON oi.order_id = o.order_id
    WHERE o.status = 'completed'
    GROUP BY 1, 2, 3, 4
),

category_totals AS (
    SELECT
        product_category,
        SUM(total_revenue) AS category_revenue,
        SUM(total_units_sold) AS category_units_sold
    FROM product_sales
    GROUP BY 1
),

overall_totals AS (
    SELECT
        SUM(total_revenue) AS total_revenue,
        SUM(total_units_sold) AS total_units_sold
    FROM product_sales
)

SELECT
    ps.*,
    ps.total_revenue / NULLIF(ps.total_units_sold, 0) AS avg_selling_price,
    ps.total_revenue / NULLIF(ps.order_count, 0) AS revenue_per_order,
    ps.total_units_sold / NULLIF(ps.order_count, 0) AS units_per_order,
    ps.total_revenue / NULLIF(ct.category_revenue, 0) * 100 AS pct_category_revenue,
    ps.total_units_sold / NULLIF(ct.category_units_sold, 0) * 100 AS pct_category_units,
    ps.total_revenue / NULLIF(ot.total_revenue, 0) * 100 AS pct_total_revenue,
    ps.total_units_sold / NULLIF(ot.total_units_sold, 0) * 100 AS pct_total_units,
    CASE
        WHEN ps.total_revenue > (ct.category_revenue * 0.25) THEN 'High'
        WHEN ps.total_revenue > (ct.category_revenue * 0.10) THEN 'Medium'
        ELSE 'Low'
    END AS category_importance,
    RANK() OVER (ORDER BY ps.total_revenue DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY ps.product_category ORDER BY ps.total_revenue DESC) AS category_revenue_rank
FROM product_sales ps
JOIN category_totals ct ON ps.product_category = ct.product_category
CROSS JOIN overall_totals ot
ORDER BY ps.total_revenue DESC
