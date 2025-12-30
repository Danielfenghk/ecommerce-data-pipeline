-- ============================================================
-- Data Marts for E-Commerce Analytics
-- ============================================================

-- ============================================================
-- SALES ANALYTICS MART
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS mart_sales_summary CASCADE;

CREATE MATERIALIZED VIEW mart_sales_summary AS
SELECT
    d.date_key,
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week,
    d.day_name,
    d.is_weekend,
    -- Order metrics
    COUNT(DISTINCT fo.order_id) as total_orders,
    COUNT(DISTINCT fo.customer_id) as unique_customers,
    SUM(fo.item_count) as total_items,
    -- Revenue metrics
    SUM(fo.subtotal) as gross_revenue,
    SUM(fo.total_amount) as net_revenue,
    SUM(fo.discount_amount) as total_discounts,
    SUM(fo.tax_amount) as total_tax,
    SUM(fo.shipping_amount) as total_shipping,
    SUM(fo.profit_amount) as total_profit,
    -- Averages
    AVG(fo.total_amount) as avg_order_value,
    AVG(fo.item_count) as avg_items_per_order,
    AVG(fo.discount_amount) as avg_discount,
    -- Order status distribution
    COUNT(CASE WHEN fo.order_status = 'completed' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN fo.order_status = 'cancelled' THEN 1 END) as cancelled_orders,
    COUNT(CASE WHEN fo.order_status = 'refunded' THEN 1 END) as refunded_orders,
    -- Customer metrics
    COUNT(CASE WHEN fo.is_first_order THEN 1 END) as new_customers,
    COUNT(CASE WHEN fo.is_repeat_customer THEN 1 END) as repeat_customers
FROM fact_orders fo
JOIN dim_date d ON fo.date_key = d.date_key
GROUP BY 
    d.date_key, d.full_date, d.year, d.quarter, d.month,
    d.month_name, d.week, d.day_name, d.is_weekend;

CREATE UNIQUE INDEX idx_mart_sales_date ON mart_sales_summary(date_key);
CREATE INDEX idx_mart_sales_year ON mart_sales_summary(year);
CREATE INDEX idx_mart_sales_month ON mart_sales_summary(year, month);

-- ============================================================
-- PRODUCT PERFORMANCE MART
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS mart_product_performance CASCADE;

CREATE MATERIALIZED VIEW mart_product_performance AS
SELECT
    p.product_sk,
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.price,
    p.price_tier,
    p.rating,
    p.rating_tier,
    p.stock_status,
    -- Sales metrics (all time)
    COUNT(DISTINCT foi.order_id) as total_orders,
    SUM(foi.quantity) as total_units_sold,
    SUM(foi.total_price) as total_revenue,
    SUM(foi.profit_amount) as total_profit,
    AVG(foi.unit_price) as avg_selling_price,
    AVG(foi.discount_percent) as avg_discount_percent,
    -- Sales metrics (last 30 days)
    COUNT(DISTINCT CASE WHEN d.full_date >= CURRENT_DATE - 30 THEN foi.order_id END) as orders_last_30d,
    SUM(CASE WHEN d.full_date >= CURRENT_DATE - 30 THEN foi.quantity ELSE 0 END) as units_sold_last_30d,
    SUM(CASE WHEN d.full_date >= CURRENT_DATE - 30 THEN foi.total_price ELSE 0 END) as revenue_last_30d,
    -- Sales metrics (last 7 days)
    COUNT(DISTINCT CASE WHEN d.full_date >= CURRENT_DATE - 7 THEN foi.order_id END) as orders_last_7d,
    SUM(CASE WHEN d.full_date >= CURRENT_DATE - 7 THEN foi.quantity ELSE 0 END) as units_sold_last_7d,
    SUM(CASE WHEN d.full_date >= CURRENT_DATE - 7 THEN foi.total_price ELSE 0 END) as revenue_last_7d,
    -- Ranking
    RANK() OVER (ORDER BY SUM(foi.total_price) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(foi.quantity) DESC) as units_sold_rank,
    RANK() OVER (PARTITION BY p.category ORDER BY SUM(foi.total_price) DESC) as category_revenue_rank
FROM dim_products p
LEFT JOIN fact_order_items foi ON p.product_sk = foi.product_sk
LEFT JOIN dim_date d ON foi.date_key = d.date_key
WHERE p.is_current = TRUE
GROUP BY 
    p.product_sk, p.product_id, p.sku, p.product_name, p.category,
    p.subcategory, p.brand, p.price, p.price_tier, p.rating,
    p.rating_tier, p.stock_status;

CREATE UNIQUE INDEX idx_mart_product_sk ON mart_product_performance(product_sk);
CREATE INDEX idx_mart_product_category ON mart_product_performance(category);
CREATE INDEX idx_mart_product_brand ON mart_product_performance(brand);
CREATE INDEX idx_mart_product_revenue_rank ON mart_product_performance(revenue_rank);

-- ============================================================
-- CUSTOMER ANALYTICS MART
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS mart_customer_analytics CASCADE;

CREATE MATERIALIZED VIEW mart_customer_analytics AS
SELECT
    c.customer_sk,
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.state,
    c.country,
    c.region,
    c.customer_segment,
    c.registration_date,
    c.tenure_months,
    c.tenure_group,
    c.rfm_segment,
    -- Purchase metrics
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.total_amount) as total_spent,
    AVG(fo.total_amount) as avg_order_value,
    SUM(fo.item_count) as total_items_purchased,
    -- Time-based metrics
    MIN(fo.order_timestamp) as first_order_date,
    MAX(fo.order_timestamp) as last_order_date,
    EXTRACT(DAY FROM NOW() - MAX(fo.order_timestamp))::INTEGER as days_since_last_order,
    -- Frequency metrics
    CASE 
        WHEN COUNT(DISTINCT fo.order_id) > 0 THEN
            c.tenure_months::FLOAT / NULLIF(COUNT(DISTINCT fo.order_id), 0)
        ELSE NULL
    END as avg_days_between_orders,
    -- Category preferences
    MODE() WITHIN GROUP (ORDER BY p.category) as favorite_category,
    MODE() WITHIN GROUP (ORDER BY p.brand) as favorite_brand,
    -- Payment preferences
    MODE() WITHIN GROUP (ORDER BY fo.payment_status) as preferred_payment,
    -- Metrics by period
    COUNT(CASE WHEN d.full_date >= CURRENT_DATE - 30 THEN fo.order_id END) as orders_last_30d,
    SUM(CASE WHEN d.full_date >= CURRENT_DATE - 30 THEN fo.total_amount ELSE 0 END) as spent_last_30d,
    COUNT(CASE WHEN d.full_date >= CURRENT_DATE - 90 THEN fo.order_id END) as orders_last_90d,
    SUM(CASE WHEN d.full_date >= CURRENT_DATE - 90 THEN fo.total_amount ELSE 0 END) as spent_last_90d,
    COUNT(CASE WHEN d.year = EXTRACT(YEAR FROM CURRENT_DATE) THEN fo.order_id END) as orders_ytd,
    SUM(CASE WHEN d.year = EXTRACT(YEAR FROM CURRENT_DATE) THEN fo.total_amount ELSE 0 END) as spent_ytd
FROM dim_customers c
LEFT JOIN fact_orders fo ON c.customer_sk = fo.customer_sk
LEFT JOIN dim_date d ON fo.date_key = d.date_key
LEFT JOIN fact_order_items foi ON fo.order_sk = foi.order_sk
LEFT JOIN dim_products p ON foi.product_sk = p.product_sk
WHERE c.is_current = TRUE
GROUP BY 
    c.customer_sk, c.customer_id, c.full_name, c.email, c.city,
    c.state, c.country, c.region, c.customer_segment, c.registration_date,
    c.tenure_months, c.tenure_group, c.rfm_segment;

CREATE UNIQUE INDEX idx_mart_customer_sk ON mart_customer_analytics(customer_sk);
CREATE INDEX idx_mart_customer_segment ON mart_customer_analytics(customer_segment);
CREATE INDEX idx_mart_customer_rfm ON mart_customer_analytics(rfm_segment);
CREATE INDEX idx_mart_customer_region ON mart_customer_analytics(region);

-- ============================================================
-- CATEGORY PERFORMANCE MART
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS mart_category_performance CASCADE;

CREATE MATERIALIZED VIEW mart_category_performance AS
SELECT
    p.category,
    p.subcategory,
    d.year,
    d.month,
    d.month_name,
    -- Product metrics
    COUNT(DISTINCT p.product_id) as product_count,
    COUNT(DISTINCT p.brand) as brand_count,
    AVG(p.price) as avg_price,
    AVG(p.rating) as avg_rating,
    -- Sales metrics
    COUNT(DISTINCT foi.order_id) as total_orders,
    COUNT(DISTINCT fo.customer_id) as unique_customers,
    SUM(foi.quantity) as total_units_sold,
    SUM(foi.total_price) as total_revenue,
    SUM(foi.profit_amount) as total_profit,
    AVG(foi.total_price) as avg_order_value,
    -- Discount metrics
    AVG(foi.discount_percent) as avg_discount_percent,
    SUM(foi.discount_amount) as total_discounts,
    -- Market share within month
    SUM(foi.total_price) / NULLIF(SUM(SUM(foi.total_price)) OVER (PARTITION BY d.year, d.month), 0) * 100 as revenue_share_pct,
    -- Growth metrics (vs previous month)
    LAG(SUM(foi.total_price)) OVER (PARTITION BY p.category ORDER BY d.year, d.month) as prev_month_revenue,
    (SUM(foi.total_price) - LAG(SUM(foi.total_price)) OVER (PARTITION BY p.category ORDER BY d.year, d.month)) / 
        NULLIF(LAG(SUM(foi.total_price)) OVER (PARTITION BY p.category ORDER BY d.year, d.month), 0) * 100 as revenue_growth_pct
FROM dim_products p
JOIN fact_order_items foi ON p.product_sk = foi.product_sk
JOIN fact_orders fo ON foi.order_sk = fo.order_sk
JOIN dim_date d ON foi.date_key = d.date_key
WHERE p.is_current = TRUE
GROUP BY p.category, p.subcategory, d.year, d.month, d.month_name;

CREATE INDEX idx_mart_cat_category ON mart_category_performance(category);
CREATE INDEX idx_mart_cat_year_month ON mart_category_performance(year, month);

-- ============================================================
-- GEOGRAPHIC SALES MART
-- ============================================================
DROP MATERIALIZED VIEW IF EXISTS mart_geographic_sales CASCADE;

CREATE MATERIALIZED VIEW mart_geographic_sales AS
SELECT
    c.country,
    c.region,
    c.state,
    c.city,
    d.year,
    d.quarter,
    -- Customer metrics
    COUNT(DISTINCT c.customer_id) as total_customers,
    COUNT(DISTINCT CASE WHEN fo.is_first_order THEN fo.customer_id END) as new_customers,
    -- Sales metrics
    COUNT(DISTINCT fo.order_id) as total_orders,
    SUM(fo.total_amount) as total_revenue,
    SUM(fo.profit_amount) as total_profit,
    AVG(fo.total_amount) as avg_order_value,
    SUM(fo.item_count) as total_items_sold,
    -- Shipping metrics
    AVG(fo.shipping_amount) as avg_shipping_cost,
    AVG(fo.days_to_deliver) as avg_delivery_days,
    -- Customer value metrics
    SUM(fo.total_amount) / NULLIF(COUNT(DISTINCT c.customer_id), 0) as revenue_per_customer,
    COUNT(fo.order_id)::FLOAT / NULLIF(COUNT(DISTINCT c.customer_id), 0) as orders_per_customer
FROM dim_customers c
JOIN fact_orders fo ON c.customer_sk = fo.customer_sk
JOIN dim_date d ON fo.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.country, c.region, c.state, c.city, d.year, d.quarter;

CREATE INDEX idx_mart_geo_country ON mart_geographic_sales(country);
CREATE INDEX idx_mart_geo_region ON mart_geographic_sales(region);
CREATE INDEX idx_mart_geo_year ON mart_geographic_sales(year);

-- ============================================================
-- REFRESH FUNCTION FOR ALL MARTS
-- ============================================================
CREATE OR REPLACE FUNCTION refresh_all_data_marts()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart_sales_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart_product_performance;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart_customer_analytics;
    REFRESH MATERIALIZED VIEW mart_category_performance;
    REFRESH MATERIALIZED VIEW mart_geographic_sales;
    
    -- Log refresh
    INSERT INTO etl_job_tracking (job_name, source_table, target_table, start_time, end_time, status)
    VALUES ('refresh_data_marts', 'fact_tables', 'mart_views', NOW(), NOW(), 'completed');
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- AGGREGATED VIEWS FOR DASHBOARDS
-- ============================================================

-- Daily KPIs View
CREATE OR REPLACE VIEW vw_daily_kpis AS
SELECT
    full_date,
    total_orders,
    unique_customers,
    gross_revenue,
    net_revenue,
    total_profit,
    avg_order_value,
    new_customers,
    repeat_customers,
    CASE WHEN unique_customers > 0 THEN 
        repeat_customers::FLOAT / unique_customers * 100 
    ELSE 0 END as repeat_customer_rate,
    CASE WHEN total_orders > 0 THEN 
        cancelled_orders::FLOAT / total_orders * 100 
    ELSE 0 END as cancellation_rate
FROM mart_sales_summary
ORDER BY full_date DESC;

-- Top Products View
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    product_name,
    category,
    brand,
    price,
    total_orders,
    total_units_sold,
    total_revenue,
    total_profit,
    revenue_rank,
    revenue_last_30d,
    units_sold_last_30d
FROM mart_product_performance
WHERE revenue_rank <= 100
ORDER BY revenue_rank;

-- Customer Segments View
CREATE OR REPLACE VIEW vw_customer_segments AS
SELECT
    customer_segment,
    rfm_segment,
    COUNT(*) as customer_count,
    AVG(total_orders) as avg_orders,
    AVG(total_spent) as avg_total_spent,
    AVG(avg_order_value) as avg_order_value,
    AVG(days_since_last_order) as avg_days_since_order
FROM mart_customer_analytics
GROUP BY customer_segment, rfm_segment
ORDER BY customer_segment, avg_total_spent DESC;