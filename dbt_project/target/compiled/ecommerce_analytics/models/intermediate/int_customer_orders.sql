-- int_customer_orders.sql
-- Intermediate model: Customer order aggregations



with orders as (
    select * from "ecommerce_warehouse"."public_staging"."stg_orders"
),

customer_orders as (
    select
        customer_id,
        
        -- Order counts
        count(*) as total_orders,
        count(case when is_completed then 1 end) as completed_orders,
        
        -- Revenue metrics
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        max(total_amount) as max_order_value,
        min(total_amount) as min_order_value,
        
        -- Time metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        
        -- Recency
        current_date - max(order_date)::date as days_since_last_order
        
    from orders
    group by customer_id
)

select * from customer_orders