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
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name
    from orders
    left join successful_payments p on orders.id = p.order_id
    left join customers c on orders.user_id = c.id 
    ),
-- Total number of orders per customer
customer_orders as 
    (select 
        c.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from customers c 
    left join orders on orders.user_id = c.id 
    group by 1
    ),

-- Final CTE

final as (
select
    p.*,

    -- Sales transcation sequence
    row_number() over (order by p.order_id) as transaction_seq,

    -- Customer order/sales sequence
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,

    -- New vs returning customer
    case 
        when c.first_order_date = p.order_placed_at then 'new'
        else 'return' 
    end as nvsr,

    -- We need the cumulative CLV, ie growing with each next order and not the total cumulative for each customer id
    sum(p.total_amount_paid) over (partition by p.customer_id order by p.order_placed_at) as customer_lifetime_value
    
    -- First day of sales
    c.first_order_date as fdos

from paid_orders p 
left join customer_orders as c using (customer_id)

-- simple select statement

select * from final 