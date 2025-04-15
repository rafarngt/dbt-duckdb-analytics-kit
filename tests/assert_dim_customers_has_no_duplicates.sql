-- Test para verificar que no hay clientes duplicados
with customer_data as (
    select
        customer_id,
        count(*) as customer_count
    from {{ ref('dim_customers') }}
    group by customer_id
    having count(*) > 1
)

select *
from customer_data