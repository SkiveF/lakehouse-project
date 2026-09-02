-- gold/daily_revenue.sql
-- Daily shipped-order revenue built from the Silver orders model.

with orders as (
    select * from {{ ref('stg_orders') }}
    where status = 'shipped'
)

select
    cast(timestamp as date) as order_date,
    sum(amount)             as total_revenue,
    count(order_id)         as total_orders
from orders
group by cast(timestamp as date)
order by order_date

