-- Validar que todos los precios son positivos
-- Esta prueba verifica que no hay productos con precios negativos o cero

WITH validation AS (
    SELECT
        product_id,
        product_name,
        price
    FROM {{ ref('dim_products') }}
    WHERE price <= 0
)

SELECT * FROM validation
