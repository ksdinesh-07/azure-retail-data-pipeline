-- ============================================
-- COMPLETE SYNAPSE ANALYTICS SETUP
-- ============================================

-- Step 1: Create Gold Database
CREATE DATABASE IF NOT EXISTS gold_db;
GO

USE gold_db;
GO

-- Step 2: Create Master Key for Encryption
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Retail@2024#StrongKey!';
GO

-- Step 3: Create External Data Source for Data Lake
CREATE EXTERNAL DATA SOURCE RetailDataLake
WITH (
    LOCATION = 'abfss://gold@retaildatalake2024.dfs.core.windows.net',
    TYPE = HADOOP,
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
GO

-- Step 4: Create File Format for Parquet
CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- Step 5: Create File Format for Delta
CREATE EXTERNAL FILE FORMAT DeltaFormat
WITH (
    FORMAT_TYPE = DELTA,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- Step 6: Stored Procedure to Create Views Dynamically
CREATE OR ALTER PROC CreateGoldDeltaView @ViewName NVARCHAR(100)
AS
BEGIN
    DECLARE @statement NVARCHAR(MAX)
    
    SET @statement = N'
    CREATE OR ALTER VIEW ' + @ViewName + ' AS
    SELECT *
    FROM OPENROWSET(
        BULK ''https://retaildatalake2024.dfs.core.windows.net/gold/' + @ViewName + '/'',
        FORMAT = ''DELTA''
    ) AS [result]'
    
    EXEC sp_executesql @statement
END
GO

-- Step 7: Create All Gold Views
EXEC CreateGoldDeltaView 'daily_sales_delta';
EXEC CreateGoldDeltaView 'customer_spending_delta';
EXEC CreateGoldDeltaView 'product_performance_delta';
EXEC CreateGoldDeltaView 'category_summary_delta';
EXEC CreateGoldDeltaView 'status_summary_delta';
GO

-- Step 8: Create Stored Procedure for Sales Analysis
CREATE OR ALTER PROC GetSalesAnalysis
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET @StartDate = ISNULL(@StartDate, DATEADD(MONTH, -6, GETDATE()))
    SET @EndDate = ISNULL(@EndDate, GETDATE())
    
    SELECT 
        order_date,
        total_sales,
        total_orders,
        total_items_sold,
        total_sales / NULLIF(total_orders, 0) AS avg_order_value
    FROM daily_sales_delta
    WHERE order_date BETWEEN @StartDate AND @EndDate
    ORDER BY order_date DESC
END
GO

-- Step 9: Create Stored Procedure for Customer Segmentation
CREATE OR ALTER PROC GetCustomerSegmentation
    @MinSpent DECIMAL(10,2) = 0
AS
BEGIN
    SELECT 
        customer_segment,
        COUNT(*) AS customer_count,
        SUM(total_spent) AS total_revenue,
        AVG(total_spent) AS avg_spent,
        SUM(total_orders) AS total_orders,
        AVG(avg_order_value) AS avg_order_value
    FROM customer_spending_delta
    WHERE total_spent >= @MinSpent
    GROUP BY customer_segment
    ORDER BY total_revenue DESC
END
GO

-- Step 10: Create Stored Procedure for Product Performance
CREATE OR ALTER PROC GetTopProducts
    @TopN INT = 10,
    @Category NVARCHAR(50) = NULL
AS
BEGIN
    SELECT TOP (@TopN)
        product_name,
        category,
        total_quantity_sold,
        total_revenue,
        times_ordered,
        unique_customers,
        total_revenue / NULLIF(total_quantity_sold, 0) AS avg_price
    FROM product_performance_delta
    WHERE @Category IS NULL OR category = @Category
    ORDER BY total_revenue DESC
END
GO

-- Step 11: Verify All Objects
SELECT 'Database' AS ObjectType, DB_NAME() AS Name
UNION ALL
SELECT 'Views', name FROM sys.views
UNION ALL
SELECT 'Stored Procedures', name FROM sys.procedures;
GO

-- Step 12: Sample Queries for Testing
-- Daily Sales Summary
SELECT TOP 10 * FROM daily_sales_delta ORDER BY order_date DESC;

-- Customer Spending Summary
SELECT TOP 10 * FROM customer_spending_delta ORDER BY total_spent DESC;

-- Product Performance
SELECT TOP 10 * FROM product_performance_delta ORDER BY total_revenue DESC;

-- Category Summary
SELECT * FROM category_summary_delta ORDER BY total_revenue DESC;

-- Order Status Summary
SELECT * FROM status_summary_delta;

PRINT '✅ Synapse Analytics setup complete!'
GO
