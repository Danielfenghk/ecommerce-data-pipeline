-- dim_products.sql
-- Mart model: Product dimension

{{
    config(
        materialized='table',
        tags=['marts', 'daily']
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        
        -- Natural key
        product_id,
        
        -- Product attributes
        product_name,
        category,
        subcategory,
        brand,
        
        -- Pricing
        price as current_price,
        cost,
        profit_margin_pct,
        price_tier,
        
        -- Inventory
        stock_quantity,
        is_in_stock,
        
        -- Inventory status
        case
            when stock_quantity = 0 then 'Out of Stock'
            when stock_quantity < 10 then 'Low Stock'
            when stock_quantity < 50 then 'Medium Stock'
            else 'Well Stocked'
        end as inventory_status,
        
        -- SCD fields
        true as is_current,
        current_date as effective_date,
        
        -- Metadata
        current_timestamp as _created_at,
        current_timestamp as _updated_at
        
    from products
)

select * from final