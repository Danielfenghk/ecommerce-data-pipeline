-- mart_sales_summary.sql
-- Mart model: Daily sales aggregation

{{
    config(
        materialized='table',
        tags=['marts', 'daily']
    )
}}

with orders as (
    select * from {{ ref('fact_orders') }}
),

daily_sales as (
    select
        date_key,
        order_date::date as order_date,
        
        -- Order counts
        count(*) as total_orders,
        count(case when is_completed then 1 end) as completed_orders,
        
        -- Revenue metrics
        sum(total_amount) as gross_revenue,
        sum(subtotal) as net_revenue,
        sum(tax_amount) as total_tax,
        sum(shipping_amount) as total_shipping,
        
        -- Averages
        avg(total_amount) as avg_order_value,
        
        -- Customer metrics
        count(distinct customer_key) as unique_customers,
        
        -- Order value distribution
        count(case when order_value_tier = 'Very Low' then 1 end) as orders_very_low,
        count(case when order_value_tier = 'Low' then 1 end) as orders_low,
        count(case when order_value_tier = 'Medium' then 1 end) as orders_medium,
        count(case when order_value_tier = 'High' then 1 end) as orders_high,
        count(case when order_value_tier = 'Very High' then 1 end) as orders_very_high
        
    from orders
    group by date_key, order_date::date
)

select 
    *,
    -- Completion rate
    round(100.0 * completed_orders / nullif(total_orders, 0), 2) as completion_rate,
    
    -- Metadata
    current_timestamp as _loaded_at
    
from daily_sales
order by order_date