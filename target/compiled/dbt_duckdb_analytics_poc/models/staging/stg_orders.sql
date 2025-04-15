with source as (
    select 
        *
    from "analytics_dev"."main"."raw_orders"
),

renamed as (
    select
        order_id,
        customer_id,
        CAST(order_date AS DATE) as order_date,
        total_amount,
        status,
        payment_method,
        -- Crear campos adicionales
        extract(year from CAST(order_date AS DATE)) as order_year,
        extract(month from CAST(order_date AS DATE)) as order_month,
        case
            when status = 'completed' then 1
            else 0
        end as is_completed
    from source
)

select * from renamed