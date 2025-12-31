-- stg_orders.sql
-- Staging model: Clean and standardize order data



with source as (
    select * from "ecommerce_warehouse"."public"."orders"
),

cleaned as (
    select
        -- Primary key
        order_id,
        
        -- Foreign keys
        customer_id,
        
        -- Order details
        order_date::timestamp as order_date,
        to_char(order_date::date, 'YYYYMMDD')::integer as date_key,
        
        -- Status (standardized)
        lower(trim(status)) as order_status,
        lower(trim(payment_method)) as payment_method,
        
        -- Amounts
        coalesce(subtotal, 0) as subtotal,
        coalesce(tax, 0) as tax_amount,
        coalesce(shipping_cost, 0) as shipping_amount,
        coalesce(total_amount, 0) as total_amount,
        
        -- Order value tier
        case
            when total_amount < 50 then 'Very Low'
            when total_amount < 100 then 'Low'
            when total_amount < 200 then 'Medium'
            when total_amount < 500 then 'High'
            else 'Very High'
        end as order_value_tier,
        
        -- Flags
        status = 'completed' as is_completed,
        status in ('shipped', 'completed') as is_fulfilled,
        
        -- Metadata
        created_at,
        current_timestamp as _loaded_at
        
    from source
    where order_id is not null
      and total_amount >= 0  -- Filter out invalid orders
)

select * from cleaned