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
-- Total number of orders per customer
customer_orders as 
    (select 
        customers.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from customers
    left join orders on orders.user_id = customers.id 
    group by 1
    ),

-- Final CTE

final as (
select
    paid_orders.*,

    -- Sales transcation sequence
    row_number() over (order by paid_orders.order_id) as transaction_seq,

    -- Customer order/sales sequence
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,

    -- New vs returning customer
    case 
        when customer_orders.first_order_date = paid_orders.order_placed_at then 'new'
        else 'return' 
    end as nvsr,

    -- We need the cumulative CLV, ie growing with each next order and not the total cumulative for each customer id
    sum(p.total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at) as customer_lifetime_value
    
    -- First day of sales
    customer_orders.first_order_date as fdos

from paid_orders
left join customer_orders using (customer_id)

-- simple select statement

select * from final 