with

-- Import CTEs
orders as (
    select * from {{ source('dbt_bgrigoryan', 'orders') }}
),

customers as (
    select * from {{ source('dbt_bgrigoryan', 'customers') }}
),

payments as (
    select * from {{ source('dbt_bgrigoryan', 'payments') }}
),

-- Logical CTEs

-- Total payment per order, there are multiple payments per order
successful_payments as (
    select  
        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
    ),
-- All orders, with their respective total payment
paid_orders as 
    (select 
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        successful_payments.total_amount_paid,
        successful_payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join successful_payments on orders.id = successful_payments.order_id
    left join customers on orders.user_id = customers.id 
    ),

-- Final CTE

final as (
select
    paid_orders.order_id,
    paid_orders.customer_id,
    paid_orders.order_placed_at,
    paid_orders.order_status,
    paid_orders.total_amount_paid,
    paid_orders.payment_finalized_date,
    paid_orders.customer_first_name,
    paid_orders.customer_last_name,

    -- Sales transcation sequence
    row_number() over (order by paid_orders.order_id) as transaction_seq,

    -- Customer order/sales sequence
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,

    -- New vs returning customer, not related to order status returned
    case 
        when (rank() over (partition by paid_orders.customer_id order by paid_orders.order_placed_at)) = 1 then 'new'
        else 'return' 
    end as nvsr,

    -- We need the cumulative CLV, ie growing with each next order and not the total cumulative for each customer id
    sum(paid_orders.total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at) as customer_lifetime_value,
    
    -- First day of sales
    first_value(paid_orders.order_placed_at) over(partition by customer_id order by paid_orders.order_placed_at) as fdos

from paid_orders)
--left join customer_orders on paid_orders.customer_id = customer_orders.customer_id

-- simple select statement

select * from final 
order by customer_id