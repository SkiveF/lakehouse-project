-- gold/daily_revenue.sql
-- Équivalent de silver_to_gold.py::daily_revenue()

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

