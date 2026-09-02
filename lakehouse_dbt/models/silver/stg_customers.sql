-- silver/stg_customers.sql
-- Clean and deduplicate Bronze customers for the Silver layer.
-- Bronze est append-only et partitionne par ingestion_date : la
-- deduplication porte sur tout l'historique, ingested_at departage
-- deux versions d'une meme ligne portant le meme updated_at.

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
            order by updated_at desc, ingested_at desc
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

