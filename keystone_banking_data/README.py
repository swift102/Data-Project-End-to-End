# Databricks notebook source
# DBTITLE 1,Project README
# MAGIC %md
# MAGIC # Keystone Banking Data - End-to-End Data Engineering Project
# MAGIC
# MAGIC **Author:** Vincent Chitsike  
# MAGIC **Repository:** https://github.com/swift102/Data-Project-End-to-End  
# MAGIC **Data Source:** https://github.com/inhamo/Datasets-Advanced-2026  
# MAGIC **Purpose:** Thesis project - Banking data lakehouse implementation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ **Project Structure**
# MAGIC
# MAGIC ```
# MAGIC keystone_banking_data/
# MAGIC ├── databricks_notebooks/          # All transformation notebooks
# MAGIC │   ├── 000_config                # Configuration & secrets management
# MAGIC │   ├── 100_001_ingest_banking_data    # Bronze: Raw data ingestion
# MAGIC │   ├── 101_bronze_year_coverage_analysis  # Bronze: Data quality analysis
# MAGIC │   ├── 200_silver_transformations      # Silver: Cleansing & enrichment
# MAGIC │   └── 300_gold_aggregates            # Gold: Business aggregates & KPIs
# MAGIC ├── documentation/                 # Project documentation
# MAGIC └── tests/                        # Unit & integration tests
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 **Data Architecture**
# MAGIC
# MAGIC ### **Medallion Architecture (Bronze → Silver → Gold)**
# MAGIC
# MAGIC #### **🥉 Bronze Layer** (`keystone_banking.bronze`)
# MAGIC - **Purpose:** Raw, immutable landing zone
# MAGIC - **Source:** GitHub repository clone via shallow clone
# MAGIC - **Status:** ✅ Complete (14 tables, ~1.4M rows)
# MAGIC - **Time Range:** 2019-2021 (⚠️ 2020 has 7 months only for 8 tables)
# MAGIC
# MAGIC **Tables:**
# MAGIC - Core: accounts, customers, transactions, loans, cards
# MAGIC - Events: atm_logs, account_status_events, debit_orders
# MAGIC - Relationships: account_signatories, account_product_enrollments, account_limits_history
# MAGIC - Control: account_counter, pdf_manifest, eml_manifest
# MAGIC
# MAGIC #### **🥈 Silver Layer** (`keystone_banking.silver`)
# MAGIC - **Purpose:** Cleaned, validated, conformed data
# MAGIC - **Status:** 🚧 In development
# MAGIC - **Transformations:** Deduplication, type casting, business rules, data quality flags
# MAGIC
# MAGIC #### **🥇 Gold Layer** (`keystone_banking.gold`)
# MAGIC - **Purpose:** Business-level aggregates & analytics
# MAGIC - **Status:** 🚧 Planned
# MAGIC - **Use Cases:** BI dashboards, ML features, regulatory reports
# MAGIC
# MAGIC #### **🎯 Control Layer** (`keystone_banking.control`)
# MAGIC - **Purpose:** Metadata, audit logs, lineage tracking
# MAGIC - **Status:** ✅ Created
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🚀 **Getting Started**
# MAGIC
# MAGIC ### **Prerequisites**
# MAGIC 1. Databricks workspace access
# MAGIC 2. GitHub account with repo access
# MAGIC 3. Unity Catalog permissions (Bronze: SELECT, Silver/Gold: CREATE TABLE + MODIFY)
# MAGIC
# MAGIC ### **Setup for Collaborators**
# MAGIC
# MAGIC #### **1. Clone Git Repo in Databricks**
# MAGIC ```
# MAGIC Workspace → Create → Git Folder
# MAGIC URL: https://github.com/swift102/Data-Project-End-to-End
# MAGIC Authenticate with GitHub Personal Access Token
# MAGIC ```
# MAGIC
# MAGIC #### **2. Request Unity Catalog Permissions**
# MAGIC Contact project owner (Vincent) to run GRANT statements for:
# MAGIC - `keystone_banking.bronze` (SELECT)
# MAGIC - `keystone_banking.silver` (CREATE TABLE, MODIFY, SELECT)
# MAGIC - `keystone_banking.gold` (CREATE TABLE, MODIFY, SELECT)
# MAGIC - `keystone_banking.control` (CREATE TABLE, MODIFY, SELECT)
# MAGIC
# MAGIC #### **3. Run Configuration Notebook**
# MAGIC ```python
# MAGIC dbutils.notebook.run("./databricks_notebooks/000_config", 60)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 **Notebook Execution Order**
# MAGIC
# MAGIC ### **Bronze Ingestion (One-time)**
# MAGIC 1. `000_config` - Verify configuration
# MAGIC 2. `100_001_ingest_banking_data` - Run full ingestion (~20 min)
# MAGIC 3. `101_bronze_year_coverage_analysis` - Validate data quality
# MAGIC
# MAGIC ### **Silver Transformations (Repeatable)**
# MAGIC 4. `200_silver_transformations` - Run all Silver transformations
# MAGIC
# MAGIC ### **Gold Aggregations (Scheduled)**
# MAGIC 5. `300_gold_aggregates` - Generate business metrics
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📝 **Data Quality Notes**
# MAGIC
# MAGIC ### **Known Issues**
# MAGIC - ⚠️ **2020 Data Gap:** 8 Bronze tables contain only 7 months of 2020 (Jan-Jul)
# MAGIC   - Affects: accounts, cards, loans, debit_orders, account_signatories, etc.
# MAGIC   - Strategy: Flag incomplete records in Silver layer
# MAGIC   
# MAGIC - ✅ **Complete Coverage:**
# MAGIC   - transactions: Full 2019, 2020, 2021 (12 months each)
# MAGIC   - atm_logs: Full 2019, 2020, 2021 (12 months each)
# MAGIC   - customers: Full 2019, 2020, 2021 (12 months each)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🤝 **Collaboration**
# MAGIC
# MAGIC ### **GitHub**
# MAGIC - Repository: https://github.com/swift102/Data-Project-End-to-End
# MAGIC - Collaborators have push/pull access
# MAGIC
# MAGIC ### **Databricks**
# MAGIC - Notebooks are version-controlled via Git integration
# MAGIC - Commit/push changes through Databricks UI or Git CLI
# MAGIC - Unity Catalog schemas shared via SQL GRANT statements
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 **Additional Resources**
# MAGIC
# MAGIC - [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
# MAGIC - [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)

# COMMAND ----------

