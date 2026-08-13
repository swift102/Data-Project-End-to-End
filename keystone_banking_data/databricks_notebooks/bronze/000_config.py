# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Configuration Overview
# MAGIC %md
# MAGIC # Keystone Banking - Configuration Notebook
# MAGIC
# MAGIC This notebook loads configuration from Databricks Secrets and returns a config dictionary for use by other notebooks.
# MAGIC
# MAGIC ## Required Secrets
# MAGIC
# MAGIC Before running, ensure the `keystone_banking` secret scope exists with these keys:
# MAGIC - `github_host` - GitHub base URL
# MAGIC - `github_user` - GitHub username
# MAGIC - `github_repo` - Repository name
# MAGIC - `mask_salt` - PII masking salt value
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC ```python
# MAGIC import json
# MAGIC config = json.loads(dbutils.notebook.run("path/to/000_config", 60))
# MAGIC ```
# MAGIC
# MAGIC ## Config Structure
# MAGIC
# MAGIC Returns a JSON dictionary with:
# MAGIC - GitHub repository URL and paths
# MAGIC - Unity Catalog schema paths (bronze, silver, gold, control)
# MAGIC - Masked salt value for PII protection

# COMMAND ----------

# DBTITLE 1,Configuration Setup
import json

# Load configuration from Databricks Secrets
# All sensitive values are retrieved from the 'keystone_banking' secret scope

# GitHub Repository Configuration
_GITHUB_HOST = dbutils.secrets.get(scope="keystone_banking", key="github_host")
_GITHUB_USER = dbutils.secrets.get(scope="keystone_banking", key="github_user")
_GITHUB_REPO = dbutils.secrets.get(scope="keystone_banking", key="github_repo")

# Data Masking Configuration
MASK_SALT = dbutils.secrets.get(scope="keystone_banking", key="mask_salt")

# Build configuration dictionary
config = {
    # GitHub configuration
    "github_url": f"{_GITHUB_HOST}/{_GITHUB_USER}/{_GITHUB_REPO}.git",
    "raw_path": "Files/raw",
    
    # Unity Catalog configuration
    "catalog": "keystone_banking",
    "bronze_schema": "keystone_banking.bronze",
    "silver_schema": "keystone_banking.silver",
    "gold_schema": "keystone_banking.gold",
    "control_schema": "keystone_banking.control",
    
    # Security
    "MASK_SALT": MASK_SALT,
}

# Return config for use in other notebooks
dbutils.notebook.exit(json.dumps(config))