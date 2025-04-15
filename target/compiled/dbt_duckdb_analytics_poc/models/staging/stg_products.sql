with source as (
    select 
        *
    from "analytics_dev"."main"."raw_products"
),

renamed as (
    select
        product_id,
        product_name,
        category,
        price,
        stock_quantity,
        supplier,
        -- Crear campos adicionales
        case
            when stock_quantity <= 50 then 'Low'
            when stock_quantity <= 100 then 'Medium'
            else 'High'
        end as stock_level,
        price * stock_quantity as inventory_value
    from source
)

select * from renamed