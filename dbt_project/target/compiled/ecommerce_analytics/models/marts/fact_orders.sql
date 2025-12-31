-- fact_orders.sql
-- Mart model: Order fact table



with orders as (
    select * from "ecommerce_warehouse"."public_staging"."stg_orders"
),

customers as (
    select customer_key, customer_id 
    from "ecommerce_warehouse"."public_marts"."dim_customers"
),

final as (
    select
        -- Surrogate key
        md5(cast(coalesce(cast(o.order_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as order_key,
        
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