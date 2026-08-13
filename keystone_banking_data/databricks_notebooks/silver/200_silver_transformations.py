# Databricks notebook source
# DBTITLE 1,Silver Layer Overview
# MAGIC %md
# MAGIC # Silver Layer: Data Transformations
# MAGIC
# MAGIC **Purpose:** Clean, validate, conform, and enrich Bronze data into analytics-ready Silver tables
# MAGIC
# MAGIC **Catalog:** `keystone_banking`  
# MAGIC **Source Schema:** `bronze`  
# MAGIC **Target Schema:** `silver`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Silver Layer Objectives
# MAGIC
# MAGIC ### 1. **Data Quality & Cleansing**
# MAGIC - Remove duplicates
# MAGIC - Handle nulls and missing values
# MAGIC - Fix data type issues
# MAGIC - Standardize formats (dates, strings, codes)
# MAGIC
# MAGIC ### 2. **Business Rules & Validation**
# MAGIC - Apply business logic
# MAGIC - Flag invalid records
# MAGIC - Add data quality scores
# MAGIC - Handle 2020 data gaps
# MAGIC
# MAGIC ### 3. **Data Enrichment**
# MAGIC - Join dimension tables
# MAGIC - Calculate derived fields
# MAGIC - Add business classifications
# MAGIC - Create surrogate keys
# MAGIC
# MAGIC ### 4. **Historical Tracking**
# MAGIC - SCD Type 2 for slowly changing dimensions
# MAGIC - Effective dating
# MAGIC - Audit columns (created_at, updated_at)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tables to Create
# MAGIC
# MAGIC ### Core Entities
# MAGIC - `silver_customers` - Cleaned customer master
# MAGIC - `silver_accounts` - Validated account records
# MAGIC - `silver_transactions` - Enriched transaction history
# MAGIC - `silver_loans` - Loan master with calculated fields
# MAGIC
# MAGIC ### Dimension Tables
# MAGIC - `silver_dim_date` - Date dimension
# MAGIC - `silver_dim_product` - Product hierarchy
# MAGIC - `silver_dim_channel` - Channel classification
# MAGIC
# MAGIC ### Bridge Tables  
# MAGIC - `silver_account_signatories` - Account-to-customer relationships
# MAGIC - `silver_account_products` - Account-to-product enrollment
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Quality Strategy for 2020
# MAGIC
# MAGIC ⚠️ **Known Issue:** 8 Bronze tables have only 7 months of 2020 data
# MAGIC
# MAGIC **Approach:**
# MAGIC 1. Add `data_quality_flag` column to all Silver tables
# MAGIC 2. Flag records from 2020 incomplete months
# MAGIC 3. Document gaps in control tables
# MAGIC 4. Provide complete vs. incomplete row counts in metadata

# COMMAND ----------

# DBTITLE 1,Configuration
import json
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
import datetime

# Load config from 000_config
config_json = dbutils.notebook.run("../000_config", 60)
config = json.loads(config_json)

BRONZE_SCHEMA = config["bronze_schema"]
SILVER_SCHEMA = config["silver_schema"]
CONTROL_SCHEMA = config["control_schema"]

BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

print(f"Batch ID      : {BATCH_ID}")
print(f"Bronze schema : {BRONZE_SCHEMA}")
print(f"Silver schema : {SILVER_SCHEMA}")
print(f"Control schema: {CONTROL_SCHEMA}")

# COMMAND ----------

