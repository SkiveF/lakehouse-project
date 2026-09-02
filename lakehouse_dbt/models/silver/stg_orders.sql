-- silver/stg_orders.sql
-- Deduplicate Bronze orders for the Silver layer.
-- Bronze est append-only et partitionne par ingestion_date : la
-- deduplication porte sur tout l'historique, ingested_at departage
-- deux versions d'une meme ligne portant le meme updated_at.

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
            order by updated_at desc, ingested_at desc
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

