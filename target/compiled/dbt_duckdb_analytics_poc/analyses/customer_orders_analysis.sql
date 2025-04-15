-- Análisis de patrones de compra de clientes
-- Este análisis muestra el comportamiento de compra de los clientes, 
-- incluyendo frecuencia de compra, valor del tiempo de vida, y productos preferidos

WITH customer_orders AS (
    SELECT
        c.customer_id,
        c.full_name,
        c.email,
        o.order_id,
        o.order_date,
        o.status as order_status,
        o.net_order_amount as order_total,
        p.product_id,
        p.product_name,
        p.category as product_category,
        p.price as product_price,
        oi.quantity
    FROM "analytics_dev"."main"."dim_customers" c
    LEFT JOIN "analytics_dev"."main"."fct_orders" o ON c.customer_id = o.customer_id
    LEFT JOIN "analytics_dev"."main"."stg_order_items" oi ON o.order_id = oi.order_id
    LEFT JOIN "analytics_dev"."main"."dim_products" p ON oi.product_id = p.product_id
),

customer_metrics AS (
    SELECT
        customer_id,
        full_name,
        email,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(order_total) AS total_spend,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        DATEDIFF('day', MIN(order_date), MAX(order_date)) + 1 AS customer_lifetime_days,
        SUM(order_total) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_order_value
    FROM customer_orders
    GROUP BY 1, 2, 3
),

product_preferences AS (
    SELECT
        customer_id,
        product_category,
        COUNT(*) AS purchase_count,
        SUM(quantity) AS total_quantity
    FROM customer_orders
    WHERE product_category IS NOT NULL
    GROUP BY 1, 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY purchase_count DESC) = 1
)

SELECT
    cm.*,
    pp.product_category AS preferred_category,
    pp.purchase_count AS preferred_category_purchases,
    CASE
        WHEN order_count >= 5 THEN 'High'
        WHEN order_count >= 2 THEN 'Medium'
        ELSE 'Low'
    END AS engagement_level,
    CASE
        WHEN total_spend >= 1000 THEN 'Premium'
        WHEN total_spend >= 500 THEN 'Mid-tier'
        ELSE 'Budget'
    END AS customer_value_segment
FROM customer_metrics cm
LEFT JOIN product_preferences pp ON cm.customer_id = pp.customer_id
ORDER BY total_spend DESC