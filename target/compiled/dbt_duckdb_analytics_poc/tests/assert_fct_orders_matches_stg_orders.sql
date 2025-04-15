-- Test para verificar que todos los pedidos en staging están en fact_orders
with staging_orders as (
    select
        order_id
    from "analytics_dev"."main"."stg_orders"
),

fact_orders as (
    select
        order_id
    from "analytics_dev"."main"."fct_orders"
)

select
    so.order_id
from staging_orders so
left join fact_orders fo on so.order_id = fo.order_id
where fo.order_id is null