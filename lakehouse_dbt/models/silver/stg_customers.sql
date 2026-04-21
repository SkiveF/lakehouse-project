-- silver/stg_customers.sql
-- Équivalent de bronze_to_silver.py : lowercase email + déduplication par customer_id

with source as (
    select * from {{ source('bronze', 'customers') }}
),

transformed as (
    select
        customer_id,
        lower(email) as email,
        name,
        updated_at,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as row_num
    from source
),

deduplicated as (
    select
        customer_id,
        email,
        name,
        updated_at
    from transformed
    where row_num = 1
)

select * from deduplicated

