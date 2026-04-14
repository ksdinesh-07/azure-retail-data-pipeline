-- Create database
CREATE DATABASE gold_db;
GO

USE gold_db;
GO

-- Create stored procedure for dynamic views
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

-- Create Gold views
EXEC CreateGoldDeltaView 'daily_sales_delta';
EXEC CreateGoldDeltaView 'customer_spending_delta';
EXEC CreateGoldDeltaView 'product_performance_delta';
EXEC CreateGoldDeltaView 'category_summary_delta';
EXEC CreateGoldDeltaView 'status_summary_delta';
GO

-- Verify views
SELECT name, create_date FROM sys.views;
GO
