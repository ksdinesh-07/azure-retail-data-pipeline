-- ============================================
-- SAMPLE QUERIES FOR SYNAPSE ANALYTICS
-- ============================================

-- 1. Daily Sales Trend (Last 30 days)
SELECT 
    order_date,
    total_sales,
    total_orders,
    total_sales / NULLIF(total_orders, 0) AS avg_order_value
FROM daily_sales_delta
WHERE order_date >= DATEADD(DAY, -30, GETDATE())
ORDER BY order_date DESC;

-- 2. Monthly Sales Summary
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(total_sales) AS monthly_sales,
    SUM(total_orders) AS monthly_orders,
    AVG(total_sales / NULLIF(total_orders, 0)) AS avg_order_value
FROM daily_sales_delta
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year DESC, month DESC;

-- 3. Customer Lifetime Value (CLV) Analysis
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    SUM(total_spent) AS total_clv,
    AVG(total_spent) AS avg_clv,
    SUM(total_orders) AS total_orders,
    SUM(total_items) AS total_items
FROM customer_spending_delta
GROUP BY customer_segment
ORDER BY total_clv DESC;

-- 4. Top 5 Products by Revenue
SELECT TOP 5
    product_name,
    category,
    total_revenue,
    total_quantity_sold,
    times_ordered
FROM product_performance_delta
ORDER BY total_revenue DESC;

-- 5. Category Performance
SELECT 
    category,
    total_revenue,
    total_quantity,
    unique_products,
    total_orders,
    total_revenue / NULLIF(total_quantity, 0) AS avg_price_per_unit
FROM category_summary_delta
ORDER BY total_revenue DESC;

-- 6. Order Fulfillment Rate
SELECT 
    status,
    total_orders,
    total_revenue,
    CAST(total_orders AS FLOAT) / SUM(total_orders) OVER() * 100 AS percentage_of_orders
FROM status_summary_delta
ORDER BY total_orders DESC;

-- 7. High-Value Customers (Top 10% by spending)
SELECT TOP 10 PERCENT
    customer_id,
    first_name,
    last_name,
    total_spent,
    total_orders,
    avg_order_value
FROM customer_spending_delta
WHERE total_spent > 0
ORDER BY total_spent DESC;

-- 8. Products Never Ordered (if you have product master)
SELECT 
    p.product_id,
    p.product_name,
    p.category
FROM product_performance_delta p
LEFT JOIN order_items o ON p.product_id = o.product_id
WHERE o.order_id IS NULL;

-- 9. Running Total of Sales
SELECT 
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM daily_sales_delta
ORDER BY order_date;

-- 10. Week-over-Week Sales Comparison
WITH weekly_sales AS (
    SELECT 
        DATEPART(WEEK, order_date) AS week_number,
        SUM(total_sales) AS weekly_sales,
        MIN(order_date) AS week_start
    FROM daily_sales_delta
    GROUP BY DATEPART(WEEK, order_date)
)
SELECT 
    week_number,
    week_start,
    weekly_sales,
    LAG(weekly_sales) OVER (ORDER BY week_number) AS previous_week_sales,
    (weekly_sales - LAG(weekly_sales) OVER (ORDER BY week_number)) / 
        NULLIF(LAG(weekly_sales) OVER (ORDER BY week_number), 0) * 100 AS wow_growth_percent
FROM weekly_sales
ORDER BY week_number DESC;
