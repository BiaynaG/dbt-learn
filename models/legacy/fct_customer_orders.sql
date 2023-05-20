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
)

-- Logical CTEs

-- Final CTE



----
with 
paid_oders as 
    (select 
        oders.id as order_id,
        oders.user_id as customer_id,
        oders.order_date as order_placed_at,
        oders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name as customer_first_name,
        c.last_name as customer_last_name

    from oders
    left join 
    (select  
        orderid as order_id, 
        max(created) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1
    ) p on oders.id = p.order_id
    left join analytics.dbt_bgrigoryan.customers c on oders.user_id = c.id 
    ),

customer_oders as 
    (select 
        c.id as customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(oders.id) as number_of_oders
    from customers c 
    
    left join oders
    on oders.user_id = c.id 
    group by 1
    )

select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    case 
        when c.first_order_date = p.order_placed_at then 'new'
        else 'return' 
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_oders p 
left join customer_oders as c using (customer_id)
    
left outer join
(
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad

    from paid_oders p
    left join paid_oders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id
order by order_id