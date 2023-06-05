with

-- Import CTEs
orders as (
    select * from {{ ref('stg_dbt_bgrigoryan__orders') }}
),

customers as (
    select * from {{ ref('stg_dbt_bgrigoryan__customers') }}
),

payments as (
    select * from {{ ref('stg_dbt_bgrigoryan__payments') }}
),

-- Logical CTEs

-- Total payment per order, there are multiple payments per order
successful_payments as (
    select  
        orderid as order_id, 
        max(payment_created_at) as payment_finalized_date, 
        sum(payment_amount) / 100.0 as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1
    ),
-- All orders, with their respective total payment
paid_orders as ( 

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,

        successful_payments.total_amount_paid,
        successful_payments.payment_finalized_date,

        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join successful_payments on orders.order_id = successful_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 
    ),

-- Final CTE

final as (
select
    order_id,
        customer_id,
        order_placed_at,
        order_status,
        total_amount_paid,
        payment_finalized_date,
        customer_first_name,
        customer_last_name,

    -- Sales transcation sequence
    row_number() over (order by paid_orders.order_id) as transaction_seq,

    -- Customer order/sales sequence
    row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,

    -- New vs returning customer, not related to order status returned
    case 
        when (rank() over (partition by paid_orders.customer_id order by paid_orders.order_placed_at)) = 1 then 'new'
        else 'return' 
    end as nvsr,

    -- We need the cumulative CLV, ie growing with each next order and not the total cumulative for each customer id
    sum(total_amount_paid) over (partition by customer_id order by order_placed_at) as customer_lifetime_value,
    
    -- First day of sales
    first_value(order_placed_at) over(partition by customer_id order by order_placed_at) as fdos

from paid_orders)
--left join customer_orders on paid_orders.customer_id = customer_orders.customer_id

-- simple select statement

select * from final 
order by customer_id