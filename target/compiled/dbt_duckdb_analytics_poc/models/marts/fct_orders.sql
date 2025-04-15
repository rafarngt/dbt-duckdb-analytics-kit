with orders as (
    select 
        *
    from "analytics_dev"."main"."stg_orders"
),

order_items as (
    select
        order_id,
        sum(quantity) as total_items,
        sum(gross_amount) as gross_order_amount,
        sum(net_amount) as net_order_amount
    from "analytics_dev"."main"."stg_order_items"
    group by order_id
),

customers as (
    select
        *
    from "analytics_dev"."main"."stg_customers"
),

final as (
    select
        o.order_id,
        o.customer_id,
        c.full_name as customer_name,
        c.country as customer_country,
        o.order_date,
        o.order_year,
        o.order_month,
        o.status,
        o.payment_method,
        o.total_amount as order_total_amount,
        oi.total_items,
        oi.gross_order_amount,
        oi.net_order_amount,
        o.is_completed,
        case
            when o.total_amount > 200 then 'Large'
            when o.total_amount > 50 then 'Medium'
            else 'Small'
        end as order_size
    from orders o
    join order_items oi on o.order_id = oi.order_id
    join customers c on o.customer_id = c.customer_id
)

select * from final