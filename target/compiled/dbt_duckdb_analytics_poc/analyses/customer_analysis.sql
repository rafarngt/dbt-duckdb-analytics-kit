-- Análisis de clientes y sus patrones de compra
-- Este análisis evalúa el comportamiento de compra de los clientes

SELECT 
    c.customer_id,
    c.full_name,
    c.country,
    COUNT(o.order_id) AS total_orders,
    SUM(o.net_order_amount) AS total_spent,
    AVG(o.net_order_amount) AS avg_order_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    DATEDIFF('day', MIN(o.order_date), MAX(o.order_date)) AS days_as_customer,
    -- Frecuencia y recencia
    CASE
        WHEN COUNT(o.order_id) > 3 THEN 'Alto'
        WHEN COUNT(o.order_id) > 1 THEN 'Medio'
        ELSE 'Bajo'
    END AS frequency_segment,
    CASE
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) < 30 THEN 'Reciente'
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) < 90 THEN 'Regular'
        ELSE 'Inactivo'
    END AS recency_segment
FROM 
    "analytics_dev"."main"."dim_customers" c
LEFT JOIN 
    "analytics_dev"."main"."fct_orders" o ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, c.full_name, c.country
ORDER BY 
    total_spent DESC