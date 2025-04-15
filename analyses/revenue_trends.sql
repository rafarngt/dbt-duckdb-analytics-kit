-- Análisis de tendencias de ingresos
-- Este análisis examina las tendencias de ingresos a lo largo del tiempo,
-- mostrando patrones mensuales, trimestrales y anuales

WITH daily_sales AS (
    SELECT
        o.order_date,
        DATE_TRUNC('month', o.order_date) AS month_date,
        DATE_TRUNC('quarter', o.order_date) AS quarter_date,
        DATE_TRUNC('year', o.order_date) AS year_date,
        p.category AS product_category,
        SUM(o.net_order_amount) AS daily_revenue,
        COUNT(DISTINCT o.order_id) AS order_count,
        COUNT(DISTINCT o.customer_id) AS customer_count,
        SUM(o.net_order_amount) / NULLIF(COUNT(DISTINCT o.order_id), 0) AS avg_order_value
    FROM {{ ref('fct_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
    WHERE o.status = 'completed'
    GROUP BY 1, 2, 3, 4, 5
),

monthly_sales AS (
    SELECT
        month_date,
        product_category,
        SUM(daily_revenue) AS monthly_revenue,
        SUM(order_count) AS monthly_orders,
        COUNT(DISTINCT order_date) AS active_days,
        SUM(customer_count) AS monthly_customers,
        SUM(daily_revenue) / NULLIF(SUM(order_count), 0) AS monthly_aov
    FROM daily_sales
    GROUP BY 1, 2
),

quarterly_sales AS (
    SELECT
        quarter_date,
        product_category,
        SUM(daily_revenue) AS quarterly_revenue,
        SUM(order_count) AS quarterly_orders,
        COUNT(DISTINCT month_date) AS active_months,
        SUM(customer_count) AS quarterly_customers
    FROM daily_sales
    GROUP BY 1, 2
),

yearly_sales AS (
    SELECT
        year_date,
        product_category,
        SUM(daily_revenue) AS yearly_revenue,
        SUM(order_count) AS yearly_orders,
        COUNT(DISTINCT quarter_date) AS active_quarters,
        SUM(customer_count) AS yearly_customers
    FROM daily_sales
    GROUP BY 1, 2
)

-- Monthly revenue trends
SELECT
    ms.month_date,
    ms.product_category,
    ms.monthly_revenue,
    ms.monthly_orders,
    ms.active_days,
    ms.monthly_customers,
    ms.monthly_aov,
    LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_category ORDER BY ms.month_date) AS prev_month_revenue,
    ms.monthly_revenue - LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_category ORDER BY ms.month_date) AS revenue_change,
    CASE
        WHEN LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_category ORDER BY ms.month_date) = 0 THEN NULL
        ELSE (ms.monthly_revenue - LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_category ORDER BY ms.month_date)) / 
            NULLIF(LAG(ms.monthly_revenue) OVER (PARTITION BY ms.product_category ORDER BY ms.month_date), 0) * 100
    END AS revenue_pct_change,
    SUM(ms.monthly_revenue) OVER (PARTITION BY ms.product_category ORDER BY ms.month_date) AS cumulative_revenue
FROM monthly_sales ms
ORDER BY ms.product_category, ms.month_date
