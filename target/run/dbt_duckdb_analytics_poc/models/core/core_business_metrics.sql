
  
    
    

    create  table
      "analytics_dev"."main"."core_business_metrics__dbt_tmp"
  
    as (
      with orders as (
    select 
        *
    from "analytics_dev"."main"."fct_orders"
),

customers as (
    select
        *
    from "analytics_dev"."main"."dim_customers"
),

products as (
    select
        *
    from "analytics_dev"."main"."dim_products"
),

daily_sales as (
    select
        order_date,
        sum(net_order_amount) as daily_revenue,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as customer_count,
        sum(total_items) as items_sold
    from orders
    where status = 'completed'
    group by order_date
),

country_sales as (
    select
        customer_country,
        count(distinct order_id) as order_count,
        sum(net_order_amount) as total_revenue,
        count(distinct customer_id) as customer_count,
        sum(net_order_amount) / count(distinct customer_id) as revenue_per_customer
    from orders
    where status = 'completed'
    group by customer_country
),

category_sales as (
    select
        p.category,
        count(distinct o.order_id) as order_count,
        sum(oi.net_amount) as total_revenue,
        sum(oi.quantity) as quantity_sold
    from "analytics_dev"."main"."stg_order_items" oi
    join "analytics_dev"."main"."stg_orders" o on oi.order_id = o.order_id
    join "analytics_dev"."main"."stg_products" p on oi.product_id = p.product_id
    where o.status = 'completed'
    group by p.category
),

payment_method_analysis as (
    select
        payment_method,
        count(distinct order_id) as order_count,
        sum(net_order_amount) as total_revenue,
        avg(net_order_amount) as avg_order_value
    from orders
    where status = 'completed'
    group by payment_method
),

final as (
    select
        'Overall' as metric_group,
        'Total Revenue' as metric_name,
        cast(sum(net_order_amount) as varchar) as metric_value
    from orders
    where status = 'completed'
    
    union all
    
    select
        'Overall' as metric_group,
        'Total Orders' as metric_name,
        cast(count(distinct order_id) as varchar) as metric_value
    from orders
    where status = 'completed'
    
    union all
    
    select
        'Overall' as metric_group,
        'Average Order Value' as metric_name,
        cast(avg(net_order_amount) as varchar) as metric_value
    from orders
    where status = 'completed'
    
    union all
    
    select
        'Overall' as metric_group,
        'Total Customers' as metric_name,
        cast(count(distinct customer_id) as varchar) as metric_value
    from customers
    
    union all
    
    select
        'Overall' as metric_group,
        'Total Products' as metric_name,
        cast(count(distinct product_id) as varchar) as metric_value
    from products
    
    union all
    
    (select
        'Time' as metric_group,
        'Date with Highest Revenue' as metric_name,
        cast(order_date as varchar) as metric_value
    from daily_sales
    order by daily_revenue desc
    limit 1)
    
    union all
    
    (select
        'Geography' as metric_group,
        'Top Country by Revenue' as metric_name,
        customer_country as metric_value
    from country_sales
    order by total_revenue desc
    limit 1)
    
    union all
    
    (select
        'Product' as metric_group,
        'Top Category by Revenue' as metric_name,
        category as metric_value
    from category_sales
    order by total_revenue desc
    limit 1)
    
    union all
    
    (select
        'Customer' as metric_group,
        'Top Payment Method' as metric_name,
        payment_method as metric_value
    from payment_method_analysis
    order by order_count desc
    limit 1)
)

select * from final
    );
  
  