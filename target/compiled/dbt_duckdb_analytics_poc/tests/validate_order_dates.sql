-- Validar que las fechas de pedidos son lógicas
-- Esta prueba verifica que todas las fechas de pedido son:
-- 1. No están en el futuro
-- 2. No son anteriores a 2015 (asumiendo que es una fecha razonable para el negocio)

WITH validation AS (
    SELECT
        order_id,
        order_date,
        CURRENT_DATE() AS today
    FROM "analytics_dev"."main"."fct_orders"
    WHERE order_date > CURRENT_DATE() OR order_date < '2015-01-01'
)

SELECT * FROM validation