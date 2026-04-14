# 🚀 Azure End-to-End Retail Data Pipeline

## Project Overview
This project implements a complete **Medallion Architecture** (Bronze → Silver → Gold) data pipeline on Microsoft Azure.

## Architecture
MySQL (On-Prem) → ADF → Data Lake (Bronze) → Databricks → Data Lake (Silver/Gold) → Synapse → Power BI

## Technologies
- Azure Data Factory
- Azure Data Lake Gen2
- Azure Databricks (PySpark)
- Azure Synapse Analytics
- Power BI
- Azure Key Vault

## Setup Instructions
1. Deploy Azure resources
2. Run ADF pipelines
3. Execute Databricks notebooks
4. Create Synapse views
5. Open Power BI dashboard

## 📸 Dashboard Screenshots

### Executive Dashboard Overview
![Dashboard Overview](images/screenshots/dashboard_overview.png)

### Sales Trend Analysis
![Sales Trend](images/screenshots/sales_trend.png)

### Customer Spending Analysis
![Customer Analysis](images/screenshots/customer_analysis.png)

### Product Performance
![Product Performance](images/screenshots/product_performance.png)

### Data Model
![Data Model](images/screenshots/data_model.png)

## 📊 Power BI Report

The interactive Power BI report is available in the [`powerbi/`](powerbi/) folder.

- **File**: `retail_dashboard.pbix`
- **Key Metrics**:
  - Total Revenue
  - Total Orders
  - Average Order Value
  - Customer Segmentation
  - Product Performance

### How to View
1. Download the `.pbix` file
2. Open with Power BI Desktop (free)
3. Refresh data (if needed)
4. Explore interactive visuals

