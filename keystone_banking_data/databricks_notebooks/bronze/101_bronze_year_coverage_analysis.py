# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Year Coverage Analysis
# MAGIC %md
# MAGIC # Bronze Layer: Year Coverage Analysis
# MAGIC
# MAGIC **Purpose:** Analyze temporal coverage across all Bronze tables in `keystone_banking.bronze`
# MAGIC
# MAGIC **Dataset:** `inhamo/Datasets-Advanced-2026` (commit `bdd39ee...`)
# MAGIC
# MAGIC ## Key Findings
# MAGIC
# MAGIC ✅ **Dataset spans 3 years:** 2019, 2020, 2021  
# MAGIC ⚠️ **2020 data is incomplete** for some entities (7 months vs. 12 for other years)  
# MAGIC 📊 **~1.4 million rows** across 14 Bronze tables
# MAGIC
# MAGIC ## Analysis Scope
# MAGIC
# MAGIC This notebook checks:
# MAGIC - Which years exist in each Bronze table
# MAGIC - Row counts per year
# MAGIC - Month completeness (12 months vs. partial year)
# MAGIC - Data quality flags (missing years, incomplete coverage)

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
import pandas as pd

BRONZE_SCHEMA = "keystone_banking.bronze"

print(f"Analyzing tables in: {BRONZE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Year Coverage Analysis
# MAGIC %md
# MAGIC ## 2. Year Coverage by Table
# MAGIC
# MAGIC Check which years (2019, 2020, 2021) exist in each Bronze table and their row counts.

# COMMAND ----------

# DBTITLE 1,Analyze all Bronze tables
# Get all bronze tables
tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").collect() 
          if r.tableName.startswith("bronze_")]

print(f"Found {len(tables)} Bronze tables\n")
print("="*80)

# Analyze each table
results = []

for table_name in sorted(tables):
    full_table = f"{BRONZE_SCHEMA}.{table_name}"
    
    try:
        # Check if table has year column
        df = spark.table(full_table)
        if "year" not in df.columns:
            print(f"\u26a0️  {table_name:45s} - No 'year' column (control table)")
            results.append({
                "table": table_name,
                "has_year": False,
                "years": "N/A",
                "total_rows": df.count(),
                "2019_rows": 0,
                "2020_rows": 0,
                "2021_rows": 0,
                "2019_months": 0,
                "2020_months": 0,
                "2021_months": 0
            })
            continue
        
        # Get year stats
        year_stats = (df
            .filter((F.col("year").isNotNull()) & (F.col("year") != ""))
            .groupBy("year")
            .agg(
                F.count("*").alias("row_count"),
                F.countDistinct("month").alias("month_count")
            )
            .orderBy("year")
            .collect())
        
        if not year_stats:
            print(f"\u26a0️  {table_name:45s} - No valid year data")
            continue
        
        # Parse results
        years_present = [r.year for r in year_stats]
        total_rows = sum(r.row_count for r in year_stats)
        
        # Build year detail dict
        year_detail = {r.year: (r.row_count, r.month_count) for r in year_stats}
        
        # Format output
        year_str = ", ".join(years_present)
        detail_parts = []
        for y in ["2019", "2020", "2021"]:
            if y in year_detail:
                rows, months = year_detail[y]
                flag = "⚠️" if months < 12 else "✅"
                detail_parts.append(f"{y}: {rows:>7,} rows ({months:>2} mo) {flag}")
        
        print(f"{table_name:45s} {total_rows:>10,} rows")
        for part in detail_parts:
            print(f"  {part}")
        print()
        
        # Store for summary
        results.append({
            "table": table_name,
            "has_year": True,
            "years": year_str,
            "total_rows": total_rows,
            "2019_rows": year_detail.get("2019", (0, 0))[0],
            "2020_rows": year_detail.get("2020", (0, 0))[0],
            "2021_rows": year_detail.get("2021", (0, 0))[0],
            "2019_months": year_detail.get("2019", (0, 0))[1],
            "2020_months": year_detail.get("2020", (0, 0))[1],
            "2021_months": year_detail.get("2021", (0, 0))[1]
        })
        
    except Exception as e:
        print(f"\u274c  {table_name:45s} - Error: {str(e)[:60]}")

print("="*80)

# COMMAND ----------

# DBTITLE 1,Summary
# MAGIC %md
# MAGIC ## 3. Summary Table
# MAGIC
# MAGIC Consolidated view of year coverage across all Bronze tables.

# COMMAND ----------

# DBTITLE 1,Display summary table
# Convert to DataFrame for better display
if results:
    summary_df = pd.DataFrame(results)
    
    # Reorder columns for clarity
    cols = ["table", "total_rows", "years", 
            "2019_rows", "2019_months", 
            "2020_rows", "2020_months",
            "2021_rows", "2021_months"]
    summary_df = summary_df[cols]
    
    # Display with pandas styling
    display(summary_df)
    
    # Quick stats
    print("\n" + "="*80)
    print("QUICK STATS")
    print("="*80)
    tables_with_year = summary_df[summary_df["years"] != "N/A"]
    print(f"Total tables analyzed    : {len(summary_df)}")
    print(f"Tables with year data    : {len(tables_with_year)}")
    print(f"Total rows (all tables)  : {summary_df['total_rows'].sum():,}")
    print(f"\nYear 2019 total          : {summary_df['2019_rows'].sum():,} rows")
    print(f"Year 2020 total          : {summary_df['2020_rows'].sum():,} rows")
    print(f"Year 2021 total          : {summary_df['2021_rows'].sum():,} rows")
    
    # Check for incomplete years
    incomplete_2020 = tables_with_year[
        (tables_with_year["2020_months"] > 0) & 
        (tables_with_year["2020_months"] < 12)
    ]
    if len(incomplete_2020) > 0:
        print(f"\n\u26a0️  Tables with incomplete 2020 data: {len(incomplete_2020)}")
        for _, row in incomplete_2020.iterrows():
            print(f"   - {row['table']:40s} ({row['2020_months']} months only)")
else:
    print("No results to display")

# COMMAND ----------

# DBTITLE 1,Data Quality Findings
# MAGIC %md
# MAGIC ## 4. Data Quality Findings
# MAGIC
# MAGIC ### ✅ Complete Coverage (12 months)
# MAGIC
# MAGIC Most tables have complete year coverage for 2019 and 2021.
# MAGIC
# MAGIC ### ⚠️ Incomplete 2020 Data
# MAGIC
# MAGIC Several tables show **only 7 months of 2020 data** instead of the full 12 months. This could indicate:
# MAGIC - Data collection issues during 2020
# MAGIC - Intentional dataset design (e.g., simulating COVID-19 banking disruption)
# MAGIC - Source system availability gaps
# MAGIC
# MAGIC ### 📊 Recommendations for Silver Layer
# MAGIC
# MAGIC 1. **Date validation:** Add explicit checks for month completeness
# MAGIC 2. **Imputation strategy:** Decide how to handle 2020 gaps (interpolate, mark as missing, or exclude)
# MAGIC 3. **Time-series analysis:** Be cautious with 2020 comparisons due to incomplete data
# MAGIC 4. **Documentation:** Flag 2020 incompleteness in data dictionaries

# COMMAND ----------

# DBTITLE 1,Grant Catalog Access to Collaborators
# MAGIC %md
# MAGIC ## 5. Collaborator Access Setup
# MAGIC
# MAGIC ### Unity Catalog Permissions
# MAGIC
# MAGIC The project owner has granted the following Unity Catalog permissions to collaborators:

# COMMAND ----------

# DBTITLE 1,Grant permissions to all layers
# MAGIC %sql
# MAGIC
# MAGIC -- COLLABORATOR: nhamo.innotaku@gmail.com (Data Engineer - Full Access)
# MAGIC GRANT USE CATALOG ON CATALOG keystone_banking TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA keystone_banking.bronze TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT CREATE TABLE, MODIFY, SELECT ON SCHEMA keystone_banking.bronze TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA keystone_banking.silver TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT CREATE TABLE, MODIFY, SELECT ON SCHEMA keystone_banking.silver TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA keystone_banking.gold TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT CREATE TABLE, MODIFY, SELECT ON SCHEMA keystone_banking.gold TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA keystone_banking.control TO `nhamo.innotaku@gmail.com`;
# MAGIC GRANT CREATE TABLE, MODIFY, SELECT ON SCHEMA keystone_banking.control TO `nhamo.innotaku@gmail.com`;

# COMMAND ----------

# DBTITLE 1,Git Collaboration Setup
# MAGIC %md
# MAGIC ## 6. Collaborator Onboarding Guide
# MAGIC
# MAGIC ### Prerequisites for Team Members
# MAGIC
# MAGIC This project uses **two separate permission systems** that must both be configured:
# MAGIC
# MAGIC 1. **GitHub Repository Access** (code versioning)
# MAGIC 2. **Databricks Unity Catalog Access** (data permissions)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📂 Repository Information
# MAGIC
# MAGIC **Project Repository:** `https://github.com/swift102/Data-Project-End-to-End`  
# MAGIC **Data Repository:** `https://github.com/inhamo/Datasets-Advanced-2026` (source data only)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🚀 Setup Steps for New Collaborators
# MAGIC
# MAGIC #### Step 1: Accept GitHub Invitation
# MAGIC
# MAGIC 1. Check your email for GitHub collaboration invitation
# MAGIC 2. Click **Accept Invitation**
# MAGIC 3. Verify access to: `https://github.com/swift102/Data-Project-End-to-End`
# MAGIC
# MAGIC #### Step 2: Clone Repository in Databricks
# MAGIC
# MAGIC 1. In Databricks workspace: **Workspace** → **Create** → **Git Folder**
# MAGIC 2. Paste repository URL: `https://github.com/swift102/Data-Project-End-to-End`
# MAGIC 3. Authenticate with your **GitHub Personal Access Token** ([Create one here](https://github.com/settings/tokens))
# MAGIC 4. Choose workspace path: `/Workspace/Users/your-email@domain.com/Data-Project-End-to-End`
# MAGIC 5. Click **Create**
# MAGIC
# MAGIC ✅ You can now edit notebooks, commit, and push changes through the Databricks Git UI
# MAGIC
# MAGIC #### Step 3: Verify Unity Catalog Access
# MAGIC
# MAGIC The project owner has granted you access to:
# MAGIC
# MAGIC - **Catalog:** `keystone_banking`
# MAGIC - **Schemas:** `bronze`, `silver`, `gold`, `control`
# MAGIC - **Permissions:** CREATE TABLE, MODIFY, SELECT (full read/write)
# MAGIC
# MAGIC Test your access by running:
# MAGIC ```sql
# MAGIC SELECT * FROM keystone_banking.bronze.bronze_accounts LIMIT 5;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📋 Access Summary
# MAGIC
# MAGIC | Component | Access Level | How to Verify |
# MAGIC |-----------|--------------|---------------|
# MAGIC | **GitHub Repo** | Collaborator | Can see `swift102/Data-Project-End-to-End` |
# MAGIC | **Databricks Git Folder** | Cloned locally | Path exists in your workspace |
# MAGIC | **Unity Catalog** | READ/WRITE all layers | Query runs without permission errors |
# MAGIC | **Notebooks** | Folder permissions | Can edit and run notebooks |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Troubleshooting
# MAGIC
# MAGIC **Cannot clone Git folder?**
# MAGIC - Ensure GitHub Personal Access Token has `repo` scope
# MAGIC - Token must not be expired
# MAGIC
# MAGIC **Cannot query tables?**
# MAGIC - Contact project owner to verify GRANT statements were executed
# MAGIC - Check you're using correct Databricks workspace account
# MAGIC
# MAGIC **Cannot run notebooks?**
# MAGIC - Request workspace folder permissions from project owner
# MAGIC - Right-click folder → Permissions → Add your email

# COMMAND ----------

# DBTITLE 1,Organizing Notebooks in Git
# MAGIC %md
# MAGIC ## 7. Project Structure
# MAGIC
# MAGIC ### Repository Organization
# MAGIC
# MAGIC The project follows a layered data architecture organized in Git:
# MAGIC
# MAGIC ```
# MAGIC /Data-Project-End-to-End/
# MAGIC   └── keystone_banking_data/
# MAGIC       └── databricks_notebooks/
# MAGIC           ├── 000_config
# MAGIC           ├── bronze/
# MAGIC           │   ├── 100_001_ingest_banking_data
# MAGIC           │   └── 101_bronze_year_coverage_analysis
# MAGIC           ├── silver/
# MAGIC           │   └── (transformation notebooks)
# MAGIC           └── gold/
# MAGIC               └── (aggregation notebooks)
# MAGIC ```
# MAGIC
# MAGIC ### Design Principles
# MAGIC
# MAGIC **Version Control:** All notebooks live within the Git folder structure
# MAGIC
# MAGIC **Layer Separation:** Bronze (ingestion), Silver (transformations), Gold (aggregations)
# MAGIC
# MAGIC **Shared Configuration:** `000_config` contains reusable parameters and paths
# MAGIC
# MAGIC **Naming Convention:** `NNN_description` where `NNN` indicates execution order
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Benefits of Git-Based Development
# MAGIC
# MAGIC ✅ **Reproducibility:** Every change is tracked with commit history  
# MAGIC ✅ **Collaboration:** Multiple team members can work simultaneously  
# MAGIC ✅ **Rollback:** Easy to revert to previous working versions  
# MAGIC ✅ **Documentation:** Commit messages explain why changes were made
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Development Workflow
# MAGIC
# MAGIC 1. **Pull** latest changes before starting work
# MAGIC 2. **Edit** notebooks in your local Git folder
# MAGIC 3. **Test** changes on sample data
# MAGIC 4. **Stage** modified notebooks in Git UI
# MAGIC 5. **Commit** with descriptive message
# MAGIC 6. **Push** to shared repository
# MAGIC
# MAGIC 💡 **Tip:** Commit frequently with clear messages (e.g., "Add Bronze validation logic" rather than "Update notebook")

# COMMAND ----------

# DBTITLE 1,Step-by-Step: Moving Notebooks
# MAGIC %md
# MAGIC ## 8. Git Operations Quick Reference
# MAGIC
# MAGIC ### Common Git Tasks in Databricks
# MAGIC
# MAGIC #### Pulling Latest Changes
# MAGIC
# MAGIC 1. Open your Git folder: `Data-Project-End-to-End`
# MAGIC 2. Click **Git** icon in left sidebar
# MAGIC 3. Click **Pull** button
# MAGIC 4. Resolve any merge conflicts if prompted
# MAGIC
# MAGIC #### Committing Your Changes
# MAGIC
# MAGIC 1. Edit notebooks as needed
# MAGIC 2. Open **Git** panel
# MAGIC 3. Review **Changed Files** list
# MAGIC 4. Click **+** to stage files you want to commit
# MAGIC 5. Enter commit message (be descriptive!)
# MAGIC 6. Click **Commit**
# MAGIC 7. Click **Push** to share with team
# MAGIC
# MAGIC #### Best Practices
# MAGIC
# MAGIC ✅ **Pull before editing:** Always sync latest changes first  
# MAGIC ✅ **Commit frequently:** Small, logical commits are easier to review  
# MAGIC ✅ **Write clear messages:** "Fix date filter bug in Bronze ingestion" > "Update notebook"  
# MAGIC ✅ **Test before pushing:** Run notebooks end-to-end before sharing  
# MAGIC ✅ **Coordinate on conflicts:** Communicate with team when editing same files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Commit Message Examples
# MAGIC
# MAGIC **Good:**
# MAGIC - `Add validation for 2020 incomplete month coverage`
# MAGIC - `Refactor Bronze year analysis to use partitioned reads`
# MAGIC - `Fix GRANT statements to include Bronze write access`
# MAGIC
# MAGIC **Avoid:**
# MAGIC - `Update`
# MAGIC - `Fix stuff`
# MAGIC - `Changes`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Resolving Merge Conflicts
# MAGIC
# MAGIC If you and another collaborator edit the same notebook:
# MAGIC
# MAGIC 1. Databricks will flag the conflict
# MAGIC 2. Open the conflicted notebook
# MAGIC 3. Look for conflict markers: `<<<<<<< HEAD`, `=======`, `>>>>>>>`
# MAGIC 4. Manually choose which version to keep (or merge both)
# MAGIC 5. Remove conflict markers
# MAGIC 6. Save, commit, and push
# MAGIC
# MAGIC 💡 **Prevention:** Communicate in team chat about which notebooks you're actively editing

# COMMAND ----------

# DBTITLE 1,Quick Reference Paths
# Workspace paths reference for team members

import os

print("📋 PROJECT PATHS REFERENCE")
print("=" * 80)

# Get current user's email from Databricks environment
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    current_user = w.current_user.me().user_name
except:
    current_user = "<your-email@domain.com>"

print(f"\n👤 CURRENT USER: {current_user}")

print("\n📂 YOUR GIT FOLDER PATH:")
git_folder = f"/Workspace/Users/{current_user}/Data-Project-End-to-End"
print(f"   {git_folder}")

print("\n📓 NOTEBOOK LOCATIONS:")
notebook_base = f"{git_folder}/keystone_banking_data/databricks_notebooks"
notebooks = [
    f"{notebook_base}/000_config",
    f"{notebook_base}/bronze/100_001_ingest_banking_data",
    f"{notebook_base}/bronze/101_bronze_year_coverage_analysis (this notebook)"
]
for nb in notebooks:
    print(f"   • {nb}")

print("\n🗄️ UNITY CATALOG PATHS:")
catalog_paths = [
    "keystone_banking.bronze.*",
    "keystone_banking.silver.*",
    "keystone_banking.gold.*",
    "keystone_banking.control.*"
]
for path in catalog_paths:
    print(f"   • {path}")

print("\n" + "=" * 80)
print("💡 TIP: Bookmark these paths for quick navigation!")