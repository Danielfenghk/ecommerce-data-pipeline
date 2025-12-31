-- stg_products.sql
-- Staging model: Clean and standardize product data



with source as (
    select * from "ecommerce_warehouse"."public"."products"
),

cleaned as (
    select
        -- Primary key
        product_id,
        
        -- Product info
        trim(name) as product_name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        trim(brand) as brand,
        
        -- Pricing
        coalesce(price, 0) as price,
        coalesce(cost, 0) as cost,
        
        -- Calculated fields
        case 
            when price > 0 then round(((price - cost) / price * 100)::numeric, 2)
            else 0 
        end as profit_margin_pct,
        
        -- Price tier classification
        case
            when price < 20 then 'Budget'
            when price < 50 then 'Standard'
            when price < 100 then 'Premium'
            else 'Luxury'
        end as price_tier,
        
        -- Inventory
        coalesce(stock_quantity, 0) as stock_quantity,
        stock_quantity > 0 as is_in_stock,
        
        -- Metadata
        created_at,
        current_timestamp as _loaded_at
        
    from source
    where product_id is not null
)

select * from cleaned