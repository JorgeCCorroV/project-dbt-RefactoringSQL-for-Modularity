with 

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
    where payment_status != 'fail'
),

orders_totals as (
    select 

        order_id,
        payment_status,
        sum(payment_amount) as order_value_dollars

    from payments
    group by 1,2
),

orders_values_joined as (
    select

        orders.*,
        orders_totals.payment_status,
        orders_totals.order_value_dollars

    from orders
    left join orders_totals
        on orders.order_id = orders_totals.order_id
)

select * from orders_values_joined