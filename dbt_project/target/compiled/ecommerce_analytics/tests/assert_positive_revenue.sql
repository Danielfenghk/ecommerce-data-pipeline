-- assert_positive_revenue.sql
-- Custom data test: Ensure no negative revenue

select
    order_id,
    total_amount
from "ecommerce_warehouse"."public_marts"."fact_orders"
where total_amount < 0