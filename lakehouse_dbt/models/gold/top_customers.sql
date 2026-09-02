-- gold/top_customers.sql
-- Top customers ranked by total order amount.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_totals as (
    select
        customer_id,
        sum(amount)    as total_spent,
        count(order_id) as total_orders
    from orders
    group by customer_id
),

ranked as (
    select
        customer_id,
        total_spent,
        total_orders,
        dense_rank() over (order by total_spent desc) as rank
    from customer_totals
)

select * from ranked
where rank <= 10

