-- fact_orders.sql
-- Mart model: Order fact table

{{
    config(
        materialized='table',
        tags=['marts', 'daily'],
        post_hook=[
            "create index if not exists idx_fact_orders_date on {{ this }} (date_key)",
            "create index if not exists idx_fact_orders_customer on {{ this }} (customer_key)"
        ]
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select customer_key, customer_id 
    from {{ ref('dim_customers') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
        
        -- Natural key
        o.order_id,
        
        -- Foreign keys
        c.customer_key,
        o.date_key,
        
        -- Order attributes
        o.order_date,
        o.order_status,
        o.payment_method,
        o.order_value_tier,
        
        -- Measures
        o.subtotal,
        o.tax_amount,
        o.shipping_amount,
        o.total_amount,
        
        -- Flags
        o.is_completed,
        o.is_fulfilled,
        
        -- Metadata
        o.created_at as order_created_at,
        current_timestamp as _loaded_at
        
    from orders o
    left join customers c on o.customer_id = c.customer_id
)

select * from final