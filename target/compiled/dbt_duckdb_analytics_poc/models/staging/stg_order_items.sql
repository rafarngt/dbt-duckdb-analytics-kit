with source as (
    select 
        *
    from "analytics_dev"."main"."raw_order_items"
),

renamed as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount,
        -- Crear campos adicionales
        quantity * unit_price as gross_amount,
        quantity * unit_price * (1 - discount) as net_amount
    from source
)

select * from renamed