-- assert_positive_revenue.sql
-- Custom data test: Ensure no negative revenue

select
    order_id,
    total_amount
from {{ ref('fact_orders') }}
where total_amount < 0