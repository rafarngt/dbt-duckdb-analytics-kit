with products as (
    select 
        *
    from {{ ref('stg_products') }}
),

product_sales as (
    select
        p.product_id,
        sum(oi.quantity) as total_quantity_sold,
        sum(oi.net_amount) as total_sales
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p on oi.product_id = p.product_id
    join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
    where o.status = 'completed'
    group by p.product_id
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        p.stock_quantity,
        p.supplier,
        p.stock_level,
        p.inventory_value,
        coalesce(ps.total_quantity_sold, 0) as total_quantity_sold,
        coalesce(ps.total_sales, 0) as total_sales,
        case
            when coalesce(ps.total_sales, 0) > 500 then 'High'
            when coalesce(ps.total_sales, 0) > 100 then 'Medium'
            else 'Low'
        end as product_performance
    from products p
    left join product_sales ps on p.product_id = ps.product_id
)

select * from final
