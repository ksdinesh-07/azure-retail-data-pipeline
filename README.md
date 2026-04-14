# 🚀 Azure Data Engineering Project – End-to-End Workflow

## 📌 Project Overview
This project demonstrates an end-to-end data engineering pipeline built using Microsoft Azure services. It covers data ingestion, storage, transformation, analytics, and reporting using a modern data architecture.

The goal is to design a scalable, secure, and efficient data pipeline for processing structured data from on-premises systems to actionable insights.

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
t_performance.png)


## 🏗️ Architecture Workflow

The pipeline follows a layered architecture:

### 1. Data Source
- On-premises SQL Server Database

### 2. Ingestion Layer
- Azure Data Factory (ADF)
- Extracts data from source systems

### 3. Storage Layer
- Azure Data Lake Gen2
- Data stored in:
  - Bronze Layer (Raw Data)
  - Silver Layer (Cleaned Data)
  - Gold Layer (Curated Data)

### 4. Transformation Layer
- Azure Databricks
- Data cleaning, transformation, and enrichment

### 5. Analytics Layer
- Azure Synapse Analytics
- Data warehousing and querying

### 6. Reporting Layer
- Power BI
- Dashboard creation and visualization

### 7. Security & Governance
- Azure Active Directory (AAD)
- Azure Key Vault

---

## 🔄 Data Flow


On-Prem SQL Server
        ↓
Azure Data Factory (Ingestion)
        ↓
Azure Data Lake Gen2
 (Bronze → Silver → Gold)
        ↓
Azure Databricks (Transformation)
        ↓
Azure Synapse Analytics
        ↓
Power BI (Reporting)


---

## 🛠️ Technologies Used

- Azure Data Factory  
- Azure Data Lake Storage Gen2  
- Azure Databricks  
- Azure Synapse Analytics  
- Power BI  
- Azure Active Directory  
- Azure Key Vault  
- SQL Server  

---

## 📊 Key Features

- End-to-end ETL pipeline  
- Layered data architecture (Bronze, Silver, Gold)  
- Scalable cloud-based solution  
- Secure data access and governance  
- Real-time analytics capability (extendable)  
- Interactive dashboards using Power BI  

---

## 📷 Project Architecture Diagram

![Architecture](./architecture.png)

---

## 🚀 How to Run the Project

1. Set up Azure Data Factory pipeline
2. Connect to SQL Server data source
3. Configure Azure Data Lake Storage Gen2
4. Create Databricks notebooks for transformation
5. Load processed data into Azure Synapse
6. Connect Power BI to Synapse for visualization

---

## 📌 Future Enhancements

- Real-time streaming using Azure Event Hub  
- CI/CD pipeline integration  
- Advanced data quality checks  
- Machine learning integration  

---

## 👨‍💻 Author

Dinesh K S  
