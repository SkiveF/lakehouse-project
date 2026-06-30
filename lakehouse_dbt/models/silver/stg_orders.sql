-- silver/stg_orders.sql
-- Deduplicate Bronze orders for the Silver layer.

with source as (
    select * from {{ source('bronze', 'orders') }}
),

deduplicated as (
    select
        order_id,
        customer_id,
        amount,
        status,
        timestamp,
        updated_at,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as row_num
    from source
),

final as (
    select
        order_id,
        customer_id,
        amount,
        status,
        timestamp,
        updated_at
    from deduplicated
    where row_num = 1
)

select * from final

