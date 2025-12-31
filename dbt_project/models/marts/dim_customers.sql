-- dim_customers.sql
-- Mart model: Customer dimension with all attributes

{{
    config(
        materialized='table',
        tags=['marts', 'daily'],
        post_hook=[
            "create index if not exists idx_dim_customers_id on {{ this }} (customer_id)",
            "analyze {{ this }}"
        ]
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,
        
        -- Natural key
        c.customer_id,
        
        -- Customer attributes
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.phone,
        
        -- Location
        c.city,
        c.state,
        c.country,
        c.postal_code,
        
        -- Segmentation
        c.customer_segment,
        
        -- Lifecycle
        c.registration_date,
        c.customer_tenure_days,
        
        -- Order metrics (from intermediate model)
        coalesce(co.total_orders, 0) as lifetime_orders,
        coalesce(co.total_revenue, 0) as lifetime_revenue,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.days_since_last_order, 9999) as days_since_last_order,
        
        -- Customer value tier
        case
            when coalesce(co.total_revenue, 0) >= 1000 then 'High Value'
            when coalesce(co.total_revenue, 0) >= 500 then 'Medium Value'
            when coalesce(co.total_revenue, 0) > 0 then 'Low Value'
            else 'No Purchases'
        end as customer_value_tier,
        
        -- Activity status
        case
            when co.days_since_last_order <= 30 then 'Active'
            when co.days_since_last_order <= 90 then 'At Risk'
            when co.days_since_last_order <= 365 then 'Dormant'
            else 'Churned'
        end as activity_status,
        
        -- SCD fields
        true as is_current,
        current_date as effective_date,
        null::date as expiration_date,
        
        -- Metadata
        current_timestamp as _created_at,
        current_timestamp as _updated_at
        
    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from final