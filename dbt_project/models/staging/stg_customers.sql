-- stg_customers.sql
-- Staging model: Clean and standardize customer data

{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        -- Primary key
        customer_id,
        
        -- Names (cleaned)
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(first_name) || ' ' || trim(last_name) as full_name,
        
        -- Contact info
        lower(trim(email)) as email,
        phone,
        
        -- Location
        trim(city) as city,
        trim(state) as state,
        trim(country) as country,
        trim(postal_code) as postal_code,
        
        -- Segmentation
        coalesce(customer_segment, 'Unknown') as customer_segment,
        
        -- Dates
        registration_date::date as registration_date,
        
        -- Calculated fields
        current_date - registration_date::date as customer_tenure_days,
        
        -- Metadata
        created_at,
        current_timestamp as _loaded_at
        
    from source
    where customer_id is not null
)

select * from cleaned