-- Análisis de historial de cambios en snapshots
-- Este análisis muestra el historial completo de cambios en los snapshots,
-- permitiendo ver la evolución de los datos a través del tiempo

WITH customer_history AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        country,
        dbt_valid_from,
        dbt_valid_to,
        CASE
            WHEN dbt_valid_to IS NULL THEN 'Actual'
            ELSE 'Histórico'
        END AS version_status,
        -- Calcular duración de cada versión
        DATEDIFF('hour', dbt_valid_from, COALESCE(dbt_valid_to, CURRENT_TIMESTAMP)) AS version_hours
    FROM {{ source('snapshots', 'customers_snapshot') }}
),

product_history AS (
    SELECT
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        supplier,
        dbt_valid_from,
        dbt_valid_to,
        CASE
            WHEN dbt_valid_to IS NULL THEN 'Actual'
            ELSE 'Histórico'
        END AS version_status,
        -- Calcular duración de cada versión
        DATEDIFF('hour', dbt_valid_from, COALESCE(dbt_valid_to, CURRENT_TIMESTAMP)) AS version_hours
    FROM {{ source('snapshots', 'products_snapshot') }}
),

-- Estadísticas sobre cambios por cliente
customer_change_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS version_count,
        MIN(dbt_valid_from) AS first_captured_at,
        MAX(dbt_valid_from) AS last_changed_at
    FROM customer_history
    GROUP BY customer_id
),

-- Estadísticas sobre cambios por producto
product_change_stats AS (
    SELECT
        product_id,
        COUNT(*) AS version_count,
        MIN(dbt_valid_from) AS first_captured_at,
        MAX(dbt_valid_from) AS last_changed_at
    FROM product_history
    GROUP BY product_id
)

-- Para usar este análisis, descomenta la sección que quieras consultar:

-- Historial completo de clientes:
SELECT 
    ch.*,
    ccs.version_count,
    ccs.first_captured_at,
    ccs.last_changed_at
FROM customer_history ch
JOIN customer_change_stats ccs ON ch.customer_id = ccs.customer_id
ORDER BY ch.customer_id, ch.dbt_valid_from DESC

-- Historial completo de productos:
-- SELECT 
--     ph.*,
--     pcs.version_count,
--     pcs.first_captured_at,
--     pcs.last_changed_at
-- FROM product_history ph
-- JOIN product_change_stats pcs ON ph.product_id = pcs.product_id
-- ORDER BY ph.product_id, ph.dbt_valid_from DESC

-- Resumen de cambios por cliente:
-- SELECT * FROM customer_change_stats ORDER BY version_count DESC

-- Resumen de cambios por producto:
-- SELECT * FROM product_change_stats ORDER BY version_count DESC 