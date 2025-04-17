# 🔷 Scalable ETL Pipeline on Azure Databricks (Medallion Architecture)

## 🧰 Tech Stack
- **Azure Data Lake Storage Gen2**
- **Azure Key Vault**
- **Azure App Registration**
- **Azure Databricks**
- **PySpark**
- **Delta Lake**

---

## 📖 Overview

This project implements a **scalable ETL pipeline** using **Azure Databricks** and follows the **Medallion Architecture** pattern (Bronze → Silver → Gold layers). It ingests and processes **employee data**, applying data quality checks, transformations, and enrichment at each stage.

---


## 🏗️ Architecture

![ETL Architecture](/Architecture.png)

This diagram illustrates the Medallion Architecture flow across Bronze, Silver, and Gold layers using Azure Databricks and Delta Lake, with secure connectivity via Key Vault and App Registration.

----

## ✨ Key Features

### 🔐 Secure Integration
- Uses **Azure App Registration** and **Azure Key Vault** to securely mount **ADLS Gen2** to **Databricks File System (DBFS)**.
- Secrets are stored securely and not hardcoded into notebooks or scripts.

### 🟫 Bronze Layer
- Raw data ingestion in **Delta Lake format**.
- Stores unprocessed data for traceability and auditing.

### 🟪 Silver Layer
- Applies **data cleaning, schema enforcement**, and **deduplication** using **PySpark**.
- Provides structured and quality-assured data.

### 🟨 Gold Layer
- Aggregates and enriches data for **business-level insights**.
- Ready for consumption by analytics or BI tools.

---

## 📊 Extensibility

While this version focuses on the data engineering pipeline, it can be extended by integrating with BI tools such as **Power BI** or **Tableau** for dashboarding and reporting.

---

## 📁 Folder Structure
```bash
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregation.py
├── configs/
│   └── secrets_config.json
├── README.md
