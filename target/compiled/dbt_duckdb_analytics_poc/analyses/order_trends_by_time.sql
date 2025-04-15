-- Análisis de tendencias de pedidos por período de tiempo
-- Este análisis evalúa las tendencias de ventas en diferentes períodos

WITH date_orders AS (
    SELECT
        order_date,
        EXTRACT('year' FROM order_date) AS year,
        EXTRACT('month' FROM order_date) AS month,
        EXTRACT('day' FROM order_date) AS day,
        EXTRACT('dow' FROM order_date) AS day_of_week,
        COUNT(order_id) AS order_count,
        SUM(net_order_amount) AS total_revenue,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(total_items) AS items_sold,
        SUM(net_order_amount) / COUNT(order_id) AS avg_order_value
    FROM
        "analytics_dev"."main"."fct_orders"
    WHERE
        status = 'completed'
    GROUP BY
        order_date
),

monthly_orders AS (
    SELECT
        year,
        month,
        DATE_TRUNC('month', order_date) AS month_date,
        SUM(order_count) AS order_count,
        SUM(total_revenue) AS total_revenue,
        SUM(unique_customers) AS unique_customers,
        SUM(items_sold) AS items_sold,
        AVG(avg_order_value) AS avg_order_value
    FROM
        date_orders
    GROUP BY
        year, month, month_date
),

daily_avg AS (
    SELECT
        day_of_week,
        AVG(order_count) AS avg_orders,
        AVG(total_revenue) AS avg_revenue
    FROM
        date_orders
    GROUP BY
        day_of_week
)

SELECT
    month_date,
    order_count,
    total_revenue,
    unique_customers,
    items_sold,
    avg_order_value,
    -- Métricas de crecimiento mes a mes
    LAG(total_revenue) OVER (ORDER BY month_date) AS prev_month_revenue,
    CASE 
        WHEN LAG(total_revenue) OVER (ORDER BY month_date) > 0 
        THEN (total_revenue - LAG(total_revenue) OVER (ORDER BY month_date)) / LAG(total_revenue) OVER (ORDER BY month_date) * 100 
        ELSE NULL 
    END AS revenue_growth_pct
FROM
    monthly_orders
ORDER BY
    month_date