# Databricks notebook source
# DBTITLE 1,Gold Layer Overview
# MAGIC %md
# MAGIC # Gold Layer: Business Aggregates & Analytics
# MAGIC
# MAGIC **Purpose:** Create business-level aggregates, KPIs, and analytics-ready datasets for BI/reporting
# MAGIC
# MAGIC **Catalog:** `keystone_banking`  
# MAGIC **Source Schema:** `silver`  
# MAGIC **Target Schema:** `gold`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Gold Layer Objectives
# MAGIC
# MAGIC ### 1. **Business Metrics & KPIs**
# MAGIC - Customer lifetime value
# MAGIC - Account profitability
# MAGIC - Transaction volumes and trends
# MAGIC - Loan portfolio health
# MAGIC
# MAGIC ### 2. **Aggregated Fact Tables**
# MAGIC - Daily/Monthly/Yearly summaries
# MAGIC - Pre-joined wide tables for BI tools
# MAGIC - Denormalized for query performance
# MAGIC
# MAGIC ### 3. **Analytical Datasets**
# MAGIC - Customer segmentation
# MAGIC - Behavioral cohorts
# MAGIC - Risk scoring
# MAGIC - Churn prediction features
# MAGIC
# MAGIC ### 4. **Reporting Tables**
# MAGIC - Executive dashboards
# MAGIC - Regulatory reports
# MAGIC - Operational metrics
# MAGIC - Audit trails
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tables to Create
# MAGIC
# MAGIC ### Time-Series Aggregates
# MAGIC - `gold_daily_transaction_summary` - Daily transaction metrics by account/channel
# MAGIC - `gold_monthly_account_activity` - Monthly account activity summary
# MAGIC - `gold_yearly_customer_metrics` - Annual customer performance
# MAGIC
# MAGIC ### Customer Analytics
# MAGIC - `gold_customer_360` - Complete customer view (wide table)
# MAGIC - `gold_customer_segments` - Customer clustering/segmentation
# MAGIC - `gold_customer_lifetime_value` - CLV calculations
# MAGIC
# MAGIC ### Product Analytics
# MAGIC - `gold_product_performance` - Product adoption and usage
# MAGIC - `gold_loan_portfolio_summary` - Loan portfolio health metrics
# MAGIC - `gold_card_usage_trends` - Card transaction patterns
# MAGIC
# MAGIC ### Operational Reports
# MAGIC - `gold_channel_performance` - Performance by channel (ATM, Online, Branch)
# MAGIC - `gold_fraud_indicators` - Transaction anomaly flags
# MAGIC - `gold_compliance_metrics` - Regulatory reporting data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Aggregation Strategy
# MAGIC
# MAGIC ### Pre-Aggregation vs. On-Demand
# MAGIC - **Pre-aggregate:** Common time slices (daily, monthly, yearly)
# MAGIC - **On-demand:** Ad-hoc queries stay in Silver layer
# MAGIC - **Materialized views:** For frequently-accessed complex queries
# MAGIC
# MAGIC ### Performance Optimization
# MAGIC - Partition by date (year, month)
# MAGIC - Cluster by commonly-filtered columns (account_id, customer_id)
# MAGIC - Z-order for multi-column queries
# MAGIC - Cache frequently-accessed Gold tables

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

SILVER_SCHEMA = config["silver_schema"]
GOLD_SCHEMA = config["gold_schema"]
CONTROL_SCHEMA = config["control_schema"]

BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

print(f"Batch ID      : {BATCH_ID}")
print(f"Silver schema : {SILVER_SCHEMA}")
print(f"Gold schema   : {GOLD_SCHEMA}")
print(f"Control schema: {CONTROL_SCHEMA}")

# COMMAND ----------

