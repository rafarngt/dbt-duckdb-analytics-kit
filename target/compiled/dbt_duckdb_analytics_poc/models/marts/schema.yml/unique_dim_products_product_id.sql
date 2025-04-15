
    
    

select
    product_id as unique_field,
    count(*) as n_records

from "analytics_dev"."main"."dim_products"
where product_id is not null
group by product_id
having count(*) > 1


