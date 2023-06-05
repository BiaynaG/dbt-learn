with


customers as (
    select * from {{ ref('stg_dbt_bgrigoryan__customers') }}
),

-- All orders, with their respective total payment
paid_orders as ( 

    select * from {{ ref('int_orders') }}
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
        customers.customer_first_name,
        customers.customer_last_name,

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
    sum(paid_orders.total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at) as customer_lifetime_value,
    
    -- First day of sales
    first_value(order_placed_at) over(partition by paid_orders.customer_id order by paid_orders.order_placed_at) as fdos

from paid_orders
left join customers on paid_orders.customer_id = customers.customer_id)

select * from final 
