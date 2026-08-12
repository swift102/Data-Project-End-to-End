> # Microsoft Fabric to Databricks Migration Summary

**Date:** August 12, 2026  
**Project:** Keystone Banking Data - End-to-End Data Engineering  
**Source:** Microsoft Fabric Notebooks  
**Target:** Databricks PySpark Notebooks  

---

## Executive Summary

**Total Notebooks to Migrate:** 38  
**Successfully Converted:** 1 (000_config)  
**Remaining:** 37  
**Automated Migration Tool Status:** Systematic failure (internal errors)  

### Status

The automated migration tool encountered internal errors on all attempts. A manual conversion pattern has been established using the `000_config` notebook as a reference implementation. This document provides comprehensive guidance for completing the remaining conversions.

---

## Successfully Converted Notebooks

### ✅ 000_config
**Location:** [databricks_notebooks/000_config](#notebook-454464539297736)  
**Key Changes:**
* Converted `mssparkutils.notebook.exit()` → `dbutils.notebook.exit()`
* Removed hardcoded GitHub credentials (replaced with environment variables)
* Removed hardcoded MASK_SALT (replaced with secrets management pattern)
* Added security documentation for Databricks Secrets

---

## Notebooks Pending Conversion

### Bronze Layer - Ingestion (3 notebooks)
1. **100_001_ingest_banking_data** - Main ingestion from GitHub
2. **100_002_ingest_initial_deposits_bronze** - Initial deposits ingestion
3. **100_001_data_profiling** - Data profiling

### Silver Layer - Transformations (17 notebooks)
4. **200_001_transform_customers_silver** - Customer dimension
5. **200_002_transform_accounts_silver** - Account dimension
6. **200_003_build_bridge_customer_account_silver** - Bridge table
7. **200_004_transform_customer_complaints_silver** - Complaints
8. **200_005_transform_collections_cases_silver** - Collections
9. **200_006_transform_account_signatories_silver** - Signatories
10. **200_007_transform_account_product_enrollments_silver** - Product enrollments (v1)
11. **200_008_transform_transactions_silver** - Transactions
12. **200_009_transform_debit_orders_silver** - Debit orders
13. **200_010_transform_loans_silver** - Loans
14. **200_011_transform_loan_participations_silver** - Loan participations
15. **200_012_transform_account_status_events_silver** - Status events
16. **200_013_transform_account_limits_history_silver** - Limits history
17. **200_014_transform_account_product_enrollments_silver** - Product enrollments (v2)
18. **200_015_transform_atm_logs_silver** - ATM logs
19. **200_016_transform_campaigns_silver** - Campaigns
20. **200_017_transform_campaign_responses_silver** - Campaign responses

### Gold Layer - Dimensional Modeling (5 notebooks)
21. **300_001_build_dim_date_gold** - Date dimension
22. **300_002_build_dim_product_gold** - Product dimension
23. **300_003_build_dim_customer_gold** - Customer dimension
24. **300_004_build_dim_account_gold** - Account dimension
25. **300_005_build_fact_transaction_gold** - Transaction fact

### Analytics & ML (1 notebook)
26. **400_001_email_requirement_discovery_ntb** - Email analytics/ML

### Investigation (1 notebook)
27. **000_dq_2020_data_gap_investigation** - Data quality investigation

---

## Key Conversion Patterns

### 1. Notebook API Changes

#### Fabric → Databricks
```python
# FABRIC
mssparkutils.notebook.run("NotebookName", timeout)
notebookutils.notebook.run("NotebookName", timeout)

# DATABRICKS
dbutils.notebook.run("NotebookName", timeout)
```

```python
# FABRIC
mssparkutils.notebook.exit(value)

# DATABRICKS
dbutils.notebook.exit(value)
```

### 2. File System Paths

#### Fabric Lakehouse Paths
```python
# FABRIC - Lakehouse Files
LH_ROOT = "/lakehouse/default/Files"
file_path = f"{LH_ROOT}/data/file.csv"

# Reading tables
spark.table("lh_bronze_banking_data.dbo.table_name")
```

#### Databricks Unity Catalog
```python
# DATABRICKS - DBFS or Volumes
file_path = "/dbfs/mnt/data/file.csv"
# OR using Unity Catalog Volumes
file_path = "/Volumes/catalog/schema/volume/data/file.csv"

# Reading tables
spark.table("catalog.schema.table_name")
```

### 3. Secrets Management

#### Hardcoded Values (DO NOT DO THIS)
```python
# ❌ NEVER commit secrets to GitHub
MASK_SALT = "keystone_2026"
GITHUB_TOKEN = "ghp_xxxx"
DB_PASSWORD = "password123"
```

#### Databricks Secrets (RECOMMENDED)
```python
# ✅ Use Databricks Secrets
MASK_SALT = dbutils.secrets.get(scope="keystone_banking", key="mask_salt")
GITHUB_TOKEN = dbutils.secrets.get(scope="github", key="access_token")
DB_PASSWORD = dbutils.secrets.get(scope="database", key="password")
```

#### Environment Variables (ALTERNATIVE)
```python
# ✅ Use environment variables with fallback
import os
MASK_SALT = os.getenv("MASK_SALT", "DEFAULT_FOR_DEV_ONLY")
```

#### Setup Databricks Secrets
```bash
# Create secret scope
databricks secrets create-scope --scope keystone_banking

# Add secrets
databricks secrets put --scope keystone_banking --key mask_salt
databricks secrets put --scope github --key access_token
```

### 4. Schema and Catalog Structure

#### Fabric Three-Part Names
```python
# Fabric: lakehouse.database_schema.table
"lh_bronze_banking_data.dbo.bronze_customers"
"lh_silver_banking_data.silver.customers"
```

#### Databricks Unity Catalog
```python
# Databricks: catalog.schema.table
"keystone_banking.bronze.customers"
"keystone_banking.silver.customers"
```

### 5. Cell Structure Parsing

Fabric notebooks are stored as `.Notebook` folders containing `notebook-content.py` files with special comments:

```python
# CELL ********************
# ... cell content ...

# MARKDOWN ********************  
# ... markdown content ...

# METADATA ********************
# META { ... }  # Ignore these
```

**Conversion Strategy:**
1. Read `notebook-content.py` from each `.Notebook` folder
2. Split on `# CELL ********************` and `# MARKDOWN ********************`
3. Remove `# METADATA ********************` blocks
4. Create Databricks notebook cells from parsed content

### 6. Lakehouse Dependencies

Fabric notebooks have lakehouse dependencies in metadata:

```python
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<guid>",
# META       "default_lakehouse_name": "lh_bronze_banking_data"
```

**Databricks Equivalent:**
* Use Unity Catalog default catalog/schema
* Or explicitly reference: `catalog.schema.table`
* Set default catalog: `spark.sql("USE CATALOG keystone_banking")`

---

## Step-by-Step Conversion Guide

### For Each Notebook:

1. **Read Source Content**
   ```python
   # Read from: keystone_banking_data/<notebook_name>.Notebook/notebook-content.py
   ```

2. **Parse Cells**
   * Split on `# CELL ********************` markers
   * Identify cell types (Python, Markdown, SQL)
   * Strip metadata blocks

3. **Apply Transformations**
   * Replace `mssparkutils` → `dbutils`
   * Replace `notebookutils` → `dbutils`
   * Update file paths (Fabric Lakehouse → Databricks paths)
   * Update table references (3-part names)
   * Parameterize secrets and credentials

4. **Create Databricks Notebook**
   ```python
   createAsset({
       "assetType": "notebook",
       "name": "databricks_notebooks/<notebook_name>"
   })
   ```

5. **Add Cells**
   ```python
   editAsset({
       "operation": "add",
       "content": [...cells...]
   })
   ```

6. **Test and Validate**
   * Run the notebook
   * Verify table references
   * Check for missing secrets
   * Validate output matches expectations

---

## Common Issues and Solutions

### Issue 1: Missing Secrets
**Symptom:** `KeyError: 'MASK_SALT'` or similar  
**Solution:** Set up Databricks Secrets or environment variables

### Issue 2: Table Not Found
**Symptom:** `Table or view not found: lh_bronze_banking_data.dbo.customers`  
**Solution:** Update to Unity Catalog format: `catalog.schema.table`

### Issue 3: Path Not Found
**Symptom:** `Path does not exist: /lakehouse/default/Files/...`  
**Solution:** Update to Databricks paths: `/dbfs/...` or Unity Catalog Volumes

### Issue 4: Module Not Found
**Symptom:** `No module named 'mssparkutils'`  
**Solution:** Replace with `dbutils`

### Issue 5: Git Operations
**Symptom:** Git clone operations in notebooks  
**Solution:** Consider using:
* Databricks Repos for Git integration
* Unity Catalog Volumes for data storage
* External locations with cloud storage

---

## Recommended Next Steps

### Phase 1: Foundation (Priority: HIGH)
1. ✅ **000_config** - Already converted
2. **Set up Unity Catalog structure**
   ```sql
   CREATE CATALOG IF NOT EXISTS keystone_banking;
   CREATE SCHEMA IF NOT EXISTS keystone_banking.bronze;
   CREATE SCHEMA IF NOT EXISTS keystone_banking.silver;
   CREATE SCHEMA IF NOT EXISTS keystone_banking.gold;
   CREATE SCHEMA IF NOT EXISTS keystone_banking.control;
   ```

3. **Configure Databricks Secrets**
   * mask_salt
   * GitHub credentials (if needed)
   * Any API keys or connection strings

### Phase 2: Bronze Layer (Priority: HIGH)
4. Convert ingestion notebooks (100_* series)
5. Test data flow from source → Bronze tables
6. Validate data quality and volumes

### Phase 3: Silver Layer (Priority: MEDIUM)
7. Convert transformation notebooks (200_* series)
8. Implement incremental processing patterns
9. Test PII masking and data quality rules

### Phase 4: Gold Layer (Priority: MEDIUM)
10. Convert dimensional modeling notebooks (300_* series)
11. Build and validate fact/dimension tables
12. Implement SCD Type 2 where needed

### Phase 5: Analytics & Validation (Priority: LOW)
13. Convert analytics notebooks (400_* series)
14. Convert investigation notebooks (000_dq_*)
15. Build dashboards and reports

---

## Security Checklist for GitHub

Before committing notebooks to GitHub, verify:

- [ ] No hardcoded passwords or API keys
- [ ] No hardcoded connection strings
- [ ] No hardcoded MASK_SALT or encryption keys
- [ ] No personally identifiable information (PII)
- [ ] No internal server names or IP addresses
- [ ] No Fabric lakehouse GUIDs or workspace IDs
- [ ] All secrets use `dbutils.secrets.get()` or environment variables
- [ ] Placeholder values clearly marked with `YOUR_*` or `REPLACE_*`
- [ ] Documentation references Databricks Secrets setup

---

## Resources

### Databricks Documentation
* [Secrets Management](https://docs.databricks.com/security/secrets/index.html)
* [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Notebook Workflows](https://docs.databricks.com/notebooks/notebook-workflows.html)
* [dbutils Reference](https://docs.databricks.com/dev-tools/databricks-utils.html)

### Migration Tools
* [Databricks Labs ucx](https://github.com/databrickslabs/ucx) - Unity Catalog migration
* [Azure Migrate](https://azure.microsoft.com/en-us/products/azure-migrate) - Fabric to Databricks

---

## Contact & Support

For questions about this migration:
1. Review the converted `000_config` notebook as a reference
2. Check the conversion patterns in this document
3. Test changes in a development workspace first
4. Reach out to the Databricks support team for complex migration scenarios

---

**Migration Status:** IN PROGRESS  
**Last Updated:** 2026-08-12  
**Next Review:** After Bronze layer conversion