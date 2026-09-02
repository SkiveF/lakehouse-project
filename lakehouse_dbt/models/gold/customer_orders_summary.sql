-- gold/customer_orders_summary.sql
-- Customer-level order metrics built from Silver models.

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders_shipped as (
    select * from {{ ref('stg_orders') }}
    where status = 'shipped'
),

orders_agg as (
    select
        customer_id,
        count(order_id)    as total_orders,
        sum(amount)        as total_amount,
        max(timestamp)     as order_date
    from orders_shipped
    group by customer_id
)

select
    c.customer_id,
    c.email,
    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.total_amount, 0) as total_amount,
    o.order_date
from customers c
left join orders_agg o on c.customer_id = o.customer_id

