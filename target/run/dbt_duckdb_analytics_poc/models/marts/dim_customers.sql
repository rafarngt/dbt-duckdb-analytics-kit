
  
    
    

    create  table
      "analytics_dev"."main"."dim_customers__dbt_tmp"
  
    as (
      with customers as (
    select 
        *
    from "analytics_dev"."main"."stg_customers"
),

customer_orders as (
    select
        customer_id,
        count(*) as order_count,
        sum(total_amount) as lifetime_value
    from "analytics_dev"."main"."stg_orders"
    group by customer_id
),

final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.registration_date,
        c.country,
        c.days_since_registration,
        coalesce(co.order_count, 0) as order_count,
        coalesce(co.lifetime_value, 0) as lifetime_value,
        case
            when coalesce(co.lifetime_value, 0) > 500 then 'High'
            when coalesce(co.lifetime_value, 0) > 100 then 'Medium'
            else 'Low'
        end as customer_value_segment
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final
    );
  
  