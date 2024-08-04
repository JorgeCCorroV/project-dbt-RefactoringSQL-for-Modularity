-- Import CTEs
with

orders as (
    select * from {{ ref('int_orders') }}
),

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),
-----
customer_orders as (

    select

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        -- Customer level aggregations
        min(orders.order_date) over(partition by orders.customer_id) as customer_first_order_date,
        min(orders.valid_order_date) over(partition by orders.customer_id) as customer_first_non_returned_order_date,
        max(orders.valid_order_date) over(partition by orders.customer_id) as customer_most_recent_non_returned_order_date,
        count(*) over(partition by orders.customer_id) as customer_order_count, --it's the same of: coalesce(max(user_order_seq),0)

        /*sum(nvl2(orders.valid_order_date, 1, 0)) over(partition by orders.customer_id) as customer_non_returned_order_count,
        this function does not work in BigQuery, so: */
        sum(case when orders.valid_order_date is not null then 1 else 0 end) over(partition by orders.customer_id) 
            as customer_non_returned_order_count,

       /* sum(nvl2(orders.valid_order_date, orders.order_value_dollars, 0)) over(partition by orders.customer_id) 
            as customer_total_lifetime_value,
        this function does not work in BigQuery, so: */
        sum(case when orders.valid_order_date is not null then orders.order_value_dollars else 0 end) 
            over(partition by orders.customer_id)
            as customer_total_lifetime_value,

        /*Let's take some notes: 
        if we add the following array_agg() function, the script does not run:
        array_agg(distinct orders.order_id) over(partition by orders.customer_id) as customer_order_ids
        However, the customer_order_ids is not being used in the final table */

    from orders
        inner join customers
        on orders.customer_id = customers.customer_id
    group by orders.order_id, orders.customer_id, orders.order_date, orders.order_status, orders.valid_order_date, 
             orders.user_order_seq, orders.payment_status, orders.order_value_dollars,
             customers.full_name, customers.surname, customers.givenname
),


add_avg_order_values as (
    select *,
        customer_total_lifetime_value / customer_non_returned_order_count as customer_avg_non_returned_order_value

    from customer_orders
),

-----
-- Final CTEs

final as (
select 

    order_id,
    customer_id,
    surname,
    givenname,
    customer_first_order_date as first_order_date,
    customer_order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    order_value_dollars,
    order_status,
    payment_status

from add_avg_order_values
)

-- Simple Select Statement

select * from final