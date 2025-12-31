
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- assert_positive_revenue.sql
-- Custom data test: Ensure no negative revenue

select
    order_id,
    total_amount
from "ecommerce_warehouse"."public_marts"."fact_orders"
where total_amount < 0
  
  
      
    ) dbt_internal_test