{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select 
    *,
    current_timestamp as updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
