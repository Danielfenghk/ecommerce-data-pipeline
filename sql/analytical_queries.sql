-- ============================================================
-- Analytical Queries for E-Commerce Data Pipeline
-- ============================================================

-- ============================================================
-- SALES ANALYSIS
-- ============================================================

-- 1. Monthly Revenue Trend with Year-over-Year Comparison
WITH monthly_revenue AS (
    SELECT
        year,
        month,
        month_name,
        SUM(net_revenue) as revenue,
        SUM(total_orders) as orders,
        SUM(unique_customers) as customers
    FROM mart_sales_summary
    GROUP BY year, month, month_name
)
SELECT
    curr.year,
    curr.month,
    curr.month_name,
    curr.revenue as current_revenue,
    prev.revenue as previous_year_revenue,
    curr.revenue - COALESCE(prev.revenue, 0) as revenue_difference,
    CASE WHEN prev.revenue > 0 THEN
        ROUND((curr.revenue - prev.revenue) / prev.revenue * 100, 2)
    ELSE NULL END as yoy_growth_pct,
    curr.orders as current_orders,
    curr.customers as current_customers
FROM monthly_revenue curr
LEFT JOIN monthly_revenue prev 
    ON curr.month = prev.month 
    AND curr.year = prev.year + 1
ORDER BY curr.year DESC, curr.month DESC;

-- 2. Daily Sales with Moving Averages
SELECT
    full_date,
    net_revenue as daily_revenue,
    total_orders as daily_orders,
    AVG(net_revenue) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7day_revenue,
    AVG(net_revenue) OVER (ORDER BY full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as ma_30day_revenue,
    AVG(total_orders) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7day_orders,
    -- Day over day change
    net_revenue - LAG(net_revenue) OVER (ORDER BY full_date) as daily_revenue_change,
    -- Week over week comparison
    net_revenue - LAG(net_revenue, 7) OVER (ORDER BY full_date) as wow_revenue_change
FROM mart_sales_summary
WHERE full_date >= CURRENT_DATE - 90
ORDER BY full_date DESC;

-- 3. Revenue by Day of Week
SELECT
    day_name,
    day_of_week,
    COUNT(*) as total_days,
    SUM(total_orders) as total_orders,
    SUM(net_revenue) as total_revenue,
    AVG(net_revenue) as avg_daily_revenue,
    AVG(avg_order_value) as avg_order_value,
    SUM(net_revenue) / SUM(SUM(net_revenue)) OVER () * 100 as revenue_share_pct
FROM mart_sales_summary
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY day_name, day_of_week
ORDER BY day_of_week;

-- ============================================================
-- PRODUCT ANALYSIS
-- ============================================================

-- 4. Product ABC Analysis (Revenue Contribution)
WITH product_revenue AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        total_revenue,
        SUM(total_revenue) OVER () as grand_total,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC) as cumulative_revenue
    FROM mart_product_performance
    WHERE total_revenue > 0
)
SELECT
    product_id,
    product_name,
    category,
    brand,
    total_revenue,
    ROUND(total_revenue / grand_total * 100, 2) as revenue_pct,
    ROUND(cumulative_revenue / grand_total * 100, 2) as cumulative_pct,
    CASE
        WHEN cumulative_revenue / grand_total <= 0.70 THEN 'A'
        WHEN cumulative_revenue / grand_total <= 0.90 THEN 'B'
        ELSE 'C'
    END as abc_class
FROM product_revenue
ORDER BY total_revenue DESC;

-- 5. Product Affinity Analysis (Frequently Bought Together)
SELECT
    p1.product_name as product_1,
    p2.product_name as product_2,
    COUNT(*) as times_bought_together,
    COUNT(*)::FLOAT / (
        SELECT COUNT(DISTINCT order_id) FROM fact_order_items
    ) * 100 as support_pct
FROM fact_order_items oi1
JOIN fact_order_items oi2 
    ON oi1.order_id = oi2.order_id 
    AND oi1.product_sk < oi2.product_sk
JOIN dim_products p1 ON oi1.product_sk = p1.product_sk
JOIN dim_products p2 ON oi2.product_sk = p2.product_sk
GROUP BY p1.product_name, p2.product_name
HAVING COUNT(*) >= 10
ORDER BY times_bought_together DESC
LIMIT 50;

-- 6. Category Performance Trend
SELECT
    category,
    year,
    month,
    total_revenue,
    total_units_sold,
    LAG(total_revenue) OVER (PARTITION BY category ORDER BY year, month) as prev_month_revenue,
    ROUND(
        (total_revenue - LAG(total_revenue) OVER (PARTITION BY category ORDER BY year, month)) /
        NULLIF(LAG(total_revenue) OVER (PARTITION BY category ORDER BY year, month), 0) * 100,
    2) as mom_growth_pct,
    revenue_share_pct
FROM mart_category_performance
WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
ORDER BY category, year DESC, month DESC;

-- ============================================================
-- CUSTOMER ANALYSIS
-- ============================================================

-- 7. Customer Cohort Analysis
WITH customer_cohorts AS (
    SELECT
        customer_sk,
        DATE_TRUNC('month', registration_date) as cohort_month,
        total_orders,
        total_spent
    FROM mart_customer_analytics
),
order_months AS (
    SELECT
        c.customer_sk,
        c.cohort_month,
        DATE_TRUNC('month', d.full_date) as order_month,
        fo.total_amount
    FROM customer_cohorts c
    JOIN fact_orders fo ON c.customer_sk = fo.customer_sk
    JOIN dim_date d ON fo.date_key = d.date_key
)
SELECT
    cohort_month,
    order_month,
    EXTRACT(MONTH FROM AGE(order_month, cohort_month))::INTEGER as months_since_signup,
    COUNT(DISTINCT customer_sk) as active_customers,
    SUM(total_amount) as cohort_revenue,
    AVG(total_amount) as avg_order_value
FROM order_months
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month;

-- 8. RFM Segment Distribution
SELECT
    rfm_segment,
    COUNT(*) as customer_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as pct_of_customers,
    SUM(total_spent) as total_revenue,
    ROUND(SUM(total_spent) / SUM(SUM(total_spent)) OVER () * 100, 2) as pct_of_revenue,
    ROUND(AVG(total_orders), 2) as avg_orders,
    ROUND(AVG(total_spent), 2) as avg_spent,
    ROUND(AVG(days_since_last_order), 0) as avg_days_since_order
FROM mart_customer_analytics
GROUP BY rfm_segment
ORDER BY total_revenue DESC;

-- 9. Customer Lifetime Value Analysis
WITH customer_ltv AS (
    SELECT
        customer_segment,
        tenure_group,
        COUNT(*) as customer_count,
        AVG(total_spent) as avg_ltv,
        AVG(total_orders) as avg_orders,
        AVG(total_spent / NULLIF(tenure_months, 0)) as avg_monthly_value
    FROM mart_customer_analytics
    WHERE tenure_months > 0
    GROUP BY customer_segment, tenure_group
)
SELECT
    customer_segment,
    tenure_group,
    customer_count,
    ROUND(avg_ltv, 2) as avg_lifetime_value,
    ROUND(avg_orders, 2) as avg_lifetime_orders,
    ROUND(avg_monthly_value, 2) as avg_monthly_value,
    ROUND(avg_monthly_value * 12, 2) as projected_annual_value
FROM customer_ltv
ORDER BY customer_segment, avg_ltv DESC;

-- 10. Customer Churn Risk Analysis
SELECT
    CASE
        WHEN days_since_last_order <= 30 THEN 'Active'
        WHEN days_since_last_order <= 60 THEN 'At Risk'
        WHEN days_since_last_order <= 90 THEN 'High Risk'
        ELSE 'Churned'
    END as churn_status,
    COUNT(*) as customer_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as pct_of_total,
    ROUND(AVG(total_spent), 2) as avg_total_spent,
    ROUND(AVG(total_orders), 2) as avg_orders,
    SUM(total_spent) as at_risk_revenue
FROM mart_customer_analytics
WHERE total_orders > 0
GROUP BY 
    CASE
        WHEN days_since_last_order <= 30 THEN 'Active'
        WHEN days_since_last_order <= 60 THEN 'At Risk'
        WHEN days_since_last_order <= 90 THEN 'High Risk'
        ELSE 'Churned'
    END
ORDER BY 
    CASE churn_status
        WHEN 'Active' THEN 1
        WHEN 'At Risk' THEN 2
        WHEN 'High Risk' THEN 3
        ELSE 4
    END;

-- ============================================================
-- GEOGRAPHIC ANALYSIS
-- ============================================================

-- 11. Regional Performance Summary
SELECT
    region,
    country,
    SUM(total_customers) as total_customers,
    SUM(new_customers) as new_customers,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    ROUND(SUM(total_revenue) / SUM(SUM(total_revenue)) OVER () * 100, 2) as revenue_share_pct,
    ROUND(AVG(avg_order_value), 2) as avg_order_value,
    ROUND(AVG(avg_delivery_days), 1) as avg_delivery_days,
    ROUND(SUM(total_revenue) / NULLIF(SUM(total_customers), 0), 2) as revenue_per_customer
FROM mart_geographic_sales
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY region, country
ORDER BY total_revenue DESC;

-- 12. City-Level Performance
SELECT
    city,
    state,
    country,
    SUM(total_customers) as customers,
    SUM(total_orders) as orders,
    SUM(total_revenue) as revenue,
    ROUND(AVG(avg_order_value), 2) as avg_order_value,
    ROUND(AVG(orders_per_customer), 2) as orders_per_customer
FROM mart_geographic_sales
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY city, state, country
HAVING SUM(total_orders) >= 10
ORDER BY revenue DESC
LIMIT 50;

-- ============================================================
-- OPERATIONAL ANALYSIS
-- ============================================================

-- 13. Order Fulfillment Analysis
SELECT
    order_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as pct_of_orders,
    SUM(total_amount) as total_value,
    AVG(days_to_ship) as avg_days_to_ship,
    AVG(days_to_deliver) as avg_days_to_deliver,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_deliver) as median_delivery_days,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY days_to_deliver) as p95_delivery_days
FROM fact_orders
WHERE order_timestamp >= CURRENT_DATE - 30
GROUP BY order_status
ORDER BY order_count DESC;

-- 14. Shipping Method Performance
SELECT
    sm.shipping_method_name,
    sm.carrier,
    COUNT(fo.order_id) as orders,
    SUM(fo.shipping_amount) as total_shipping_revenue,
    AVG(fo.shipping_amount) as avg_shipping_cost,
    AVG(fo.days_to_deliver) as avg_delivery_days,
    sm.delivery_days_min,
    sm.delivery_days_max,
    COUNT(CASE WHEN fo.days_to_deliver > sm.delivery_days_max THEN 1 END) as late_deliveries,
    ROUND(
        COUNT(CASE WHEN fo.days_to_deliver > sm.delivery_days_max THEN 1 END)::NUMERIC /
        NULLIF(COUNT(fo.order_id), 0) * 100,
    2) as late_delivery_pct
FROM dim_shipping_method sm
LEFT JOIN fact_orders fo ON sm.shipping_method_sk = fo.shipping_method_sk
WHERE fo.order_timestamp >= CURRENT_DATE - 90
GROUP BY sm.shipping_method_name, sm.carrier, sm.delivery_days_min, sm.delivery_days_max
ORDER BY orders DESC;

-- 15. Payment Method Analysis
SELECT
    pm.payment_method_name,
    pm.payment_type,
    COUNT(fo.order_id) as order_count,
    ROUND(COUNT(fo.order_id)::NUMERIC / SUM(COUNT(fo.order_id)) OVER () * 100, 2) as usage_pct,
    SUM(fo.total_amount) as total_processed,
    AVG(fo.total_amount) as avg_transaction_value,
    SUM(fo.total_amount * pm.processing_fee_percent / 100 + pm.processing_fee_fixed) as estimated_fees
FROM dim_payment_method pm
LEFT JOIN fact_orders fo ON pm.payment_method_sk = fo.payment_method_sk
WHERE fo.order_timestamp >= CURRENT_DATE - 30
GROUP BY pm.payment_method_name, pm.payment_type, pm.processing_fee_percent, pm.processing_fee_fixed
ORDER BY order_count DESC;

-- ============================================================
-- TREND ANALYSIS
-- ============================================================

-- 16. Year-to-Date vs Previous Year
WITH ytd_current AS (
    SELECT
        SUM(total_orders) as orders,
        SUM(unique_customers) as customers,
        SUM(gross_revenue) as gross_revenue,
        SUM(net_revenue) as net_revenue,
        SUM(total_profit) as profit
    FROM mart_sales_summary
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
      AND full_date <= CURRENT_DATE
),
ytd_previous AS (
    SELECT
        SUM(total_orders) as orders,
        SUM(unique_customers) as customers,
        SUM(gross_revenue) as gross_revenue,
        SUM(net_revenue) as net_revenue,
        SUM(total_profit) as profit
    FROM mart_sales_summary
    WHERE year = EXTRACT(YEAR FROM CURRENT_DATE) - 1
      AND full_date <= (CURRENT_DATE - INTERVAL '1 year')
)
SELECT
    'Orders' as metric,
    c.orders as current_ytd,
    p.orders as previous_ytd,
    c.orders - p.orders as difference,
    ROUND((c.orders - p.orders)::NUMERIC / NULLIF(p.orders, 0) * 100, 2) as yoy_change_pct
FROM ytd_current c, ytd_previous p
UNION ALL
SELECT
    'Customers',
    c.customers,
    p.customers,
    c.customers - p.customers,
    ROUND((c.customers - p.customers)::NUMERIC / NULLIF(p.customers, 0) * 100, 2)
FROM ytd_current c, ytd_previous p
UNION ALL
SELECT
    'Revenue',
    c.net_revenue,
    p.net_revenue,
    c.net_revenue - p.net_revenue,
    ROUND((c.net_revenue - p.net_revenue) / NULLIF(p.net_revenue, 0) * 100, 2)
FROM ytd_current c, ytd_previous p
UNION ALL
SELECT
    'Profit',
    c.profit,
    p.profit,
    c.profit - p.profit,
    ROUND((c.profit - p.profit) / NULLIF(p.profit, 0) * 100, 2)
FROM ytd_current c, ytd_previous p;