Employee ETL Pipeline using Medallion Architecture (Bronze, Silver, Gold Layers)
Tech Stack: Azure Data Lake Storage Gen2, Azure Key Vault, Azure App Registration, Azure Databricks, PySpark, Delta Lake

Overview:
This project implements a scalable ETL pipeline on Azure Databricks following the Medallion Architecture (Bronze-Silver-Gold layers). It ingests employee data and processes it through multiple layers, applying data cleaning, transformation, and enrichment at each stage.

Key Features:

🔐 Secure Integration: Used Azure App Registration and Key Vault to securely connect ADLS Gen2 with Databricks File System (DBFS). This ensures secure access to storage without embedding secrets.

🟫 Bronze Layer: Raw data ingestion from ADLS into Databricks using Delta format, preserving original data for traceability.

🟪 Silver Layer: Cleaned and transformed data with schema enforcement and deduplication using PySpark.

🟨 Gold Layer: Aggregated, business-ready data curated for analytical consumption and reporting.

📊 Extensibility: Can be connected to BI tools like Power BI or Tableau for dashboards (not included in this version).

Folder Structure:

bash
Copy
Edit
├── notebooks/
│   ├── 1_bronze_ingestion.py
│   ├── 2_silver_transformation.py
│   └── 3_gold_aggregation.py
├── configs/
│   └── mount_adls.py  # Uses secrets from Key Vault for mounting ADLS
├── data/
│   └── (sample employee data)
└── README.md
📄 Resume Bullet Points:
Developed an end-to-end ETL pipeline in Databricks using Medallion Architecture to process employee data across Bronze, Silver, and Gold layers with Delta Lake.

Implemented secure data lake access using Azure App Registration and Key Vault to mount ADLS Gen2 on DBFS.

Optimized PySpark workflows for data cleaning, transformation, and aggregation, enabling scalable and modular data processing.
