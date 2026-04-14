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
