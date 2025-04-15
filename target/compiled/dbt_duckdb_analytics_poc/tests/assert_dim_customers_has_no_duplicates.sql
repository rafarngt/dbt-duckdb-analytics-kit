-- Test para verificar que no hay clientes duplicados
with customer_data as (
    select
        customer_id,
        count(*) as customer_count
    from "analytics_dev"."main"."dim_customers"
    group by customer_id
    having count(*) > 1
)

select *
from customer_data