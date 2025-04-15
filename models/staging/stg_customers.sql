with source as (
    select 
        *
    from {{ ref('raw_customers') }}
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        CAST(registration_date AS DATE) as registration_date,
        country,
        -- Crear campos adicionales
        first_name || ' ' || last_name as full_name,
        current_date() - CAST(registration_date AS DATE) as days_since_registration
    from source
)

select * from renamed
