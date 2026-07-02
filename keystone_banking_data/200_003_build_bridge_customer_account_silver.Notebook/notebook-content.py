# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a03cfff1-048d-457c-8848-da958470832d",
# META       "default_lakehouse_name": "lh_silver_banking_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "a03cfff1-048d-457c-8848-da958470832d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver Transform
# 
# **Notebook:** `200_003_build_bridge_customer_account_silver`  
# **Source:** `lh_bronze_banking_data.dbo.bronze_accounts`  
# **Target:** `bridge_customer_account`  
# **Layer:** Silver  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Load from Bronze + attach Silver batch metadata |
# | 2 | Explode signatories_json into one row per relationship |
# | 3 | Derive bridge columns |
# | 4 | Write to Silver Lakehouse + validation summary |
# 
# ---
# ## Purpose
# - Builds the bridge_customer_account Silver table by exploding the
# - signatories_json array from bronze_accounts.
# - Each row = one customer ↔ account relationship.


# CELL ********************

# 0. Config & imports
import re, json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DateType, TimestampType
)
from delta.tables import DeltaTable
from datetime import datetime, timezone

# Pull config from 000_Config
config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)

SILVER_BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
TARGET          = "bridge_customer_account"
SOURCE          = "lh_bronze_banking_data.dbo.bronze_accounts"

print(f"✅ Config loaded")
print(f"   silver_batch_id : {SILVER_BATCH_ID}")
print(f"   target          : {TARGET}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Load Bronze accounts
bronze = spark.table(SOURCE)

print(f"Bronze accounts rows : {bronze.count():,}")
print(f"Columns              : {len(bronze.columns)}")

# Confirm signatories_json is populated
bronze.select(
    F.count("*").alias("total"),
    F.count("signatories_json").alias("has_signatories"),
    F.sum(F.when(F.col("signatories_json") == "[]", 1).otherwise(0)).alias("empty_array")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Exploding Strategy:
# 1. Parse JSON array → array of structs
# 1. Explode array → one row per signatory
# 1. Extract fields from struct
# 1. Join back to account-level fields we need

# CELL ********************

# 2. Explode signatories_json into one row per relationship

# signatories_json shape (per row in the sample):
# [{"customer_id": "IND20000842",
#   "signatory_role": "primary_holder",
#   "signing_rule": "single",
#   "effective_date": "2020-08-26",
#   "is_active": true}]


from pyspark.sql.types import ArrayType

signatory_schema = ArrayType(
    StructType([
        StructField("customer_id",     StringType(),  True),
        StructField("signatory_role",  StringType(),  True),
        StructField("signing_rule",    StringType(),  True),
        StructField("effective_date",  StringType(),  True),
        StructField("is_active",       BooleanType(), True),
    ])
)

exploded = (
    bronze
    # Only process rows with a populated signatories_json
    .filter(
        F.col("signatories_json").isNotNull() &
        (F.col("signatories_json") != "[]") &
        (F.col("signatories_json") != "null")
    )
    .withColumn("signatories_parsed",
        F.from_json(F.col("signatories_json"), signatory_schema)
    )
    # Explode: one row per signatory per account
    .withColumn("signatory", F.explode(F.col("signatories_parsed")))
    # Extract signatory fields
    .withColumn("customer_id",    F.col("signatory.customer_id"))
    .withColumn("signatory_role", F.col("signatory.signatory_role"))
    .withColumn("signing_rule",   F.col("signatory.signing_rule"))
    .withColumn("effective_date", F.to_date(F.col("signatory.effective_date")))
    .withColumn("is_active",      F.col("signatory.is_active"))
    # Account-level fields to carry through
    .select(
        "customer_id",
        "account_id",
        "signatory_role",
        "signing_rule",
        "effective_date",
        "is_active",
        "is_primary_account",
        "account_type",
        "account_status",
        "currency",
        "branch_code",
        "_source_file",
        "_ingest_timestamp",
        "_batch_id",
        "_commit_sha",
    )
)

print(f"Exploded rows (one per customer-account link) : {exploded.count():,}")
exploded.groupBy("signatory_role").count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Derive bridge columns 
bridge = (
    exploded

    # is_primary_holder: true only for primary_holder role
    .withColumn("is_primary_holder",
        F.col("signatory_role") == "primary_holder"
    )

    # relationship_type: clean business label for the role
    .withColumn("relationship_type",
        F.when(F.col("signatory_role") == "primary_holder",    "Primary Holder")
         .when(F.col("signatory_role") == "joint_holder",      "Joint Holder")
         .when(F.col("signatory_role") == "authorized_signatory", "Authorized Signatory")
         .when(F.col("signatory_role") == "finance_manager",   "Finance Manager")
         .when(F.col("signatory_role") == "director",          "Director")
         .otherwise("Other")
    )

    # Audit columns
    .withColumn("silver_batch_id",       F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp", F.current_timestamp())

    # Final column selection — clean schema
    .select(
        "customer_id",
        "account_id",
        "relationship_type",
        "signatory_role",
        "signing_rule",
        "effective_date",
        "is_active",
        "is_primary_holder",
        "is_primary_account",
        "account_type",
        "account_status",
        "currency",
        "branch_code",
        "_source_file",
        "_ingest_timestamp",
        "_batch_id",
        "_commit_sha",
        "silver_batch_id",
        "silver_load_timestamp",
    )
)

print("✅ Bridge derived columns applied")
print(f"   Total rows          : {bridge.count():,}")
print(f"   Distinct customers  : {bridge.select('customer_id').distinct().count():,}")
print(f"   Distinct accounts   : {bridge.select('account_id').distinct().count():,}")

bridge.groupBy("relationship_type").count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Write to Silver

# Bridge is a full-replace table — no SCD Type 2 needed.
# Reason: it's derived entirely from Bronze signatories_json.
# On each pipeline run, we rebuild from source.

(
    bridge
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

print(f"✅ {TARGET} written to Silver")

# 5. Verify
result = spark.table(TARGET)

print(f"\nFinal row count      : {result.count():,}")
print(f"Distinct customers   : {result.select('customer_id').distinct().count():,}")
print(f"Distinct accounts    : {result.select('account_id').distinct().count():,}")

result.select(
    "customer_id", "account_id", "relationship_type",
    "is_primary_holder", "is_active", "effective_date"
).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 6. DQ checks 
print("=== DQ: Null customer_id ===")
spark.table(TARGET).filter(F.col("customer_id").isNull()).count()

print("=== DQ: Null account_id ===")
spark.table(TARGET).filter(F.col("account_id").isNull()).count()

print("=== DQ: Accounts with no primary_holder ===")
(
    spark.table(TARGET)
    .groupBy("account_id")
    .agg(F.sum(F.when(F.col("is_primary_holder"), 1).otherwise(0)).alias("primary_count"))
    .filter(F.col("primary_count") == 0)
    .count()
)

print("=== DQ: Accounts with more than one primary_holder ===")
(
    spark.table(TARGET)
    .groupBy("account_id")
    .agg(F.sum(F.when(F.col("is_primary_holder"), 1).otherwise(0)).alias("primary_count"))
    .filter(F.col("primary_count") > 1)
    .count()
)

print("=== Relationship type distribution ===")
spark.table(TARGET).groupBy("relationship_type").count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete row from both control tables

spark.sql("""
    DELETE FROM lh_silver_banking_data.control.batch_watermark
    WHERE pipeline_name = '200_003_build_bridge_customer_account_silver'
""")

spark.sql("""
    DELETE FROM lh_silver_banking_data.control.silver_audit_log
    WHERE pipeline_name = '200_003_build_bridge_customer_account_silver'
""")

print("✅ Deleted customers rows from both control tables")

# Verify
spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_silver_banking_data.control.batch_watermark
    WHERE pipeline_name = '200_003_build_bridge_customer_account_silver'
""").show(truncate=False)

spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_silver_banking_data.control.silver_audit_log
    WHERE pipeline_name = '200_003_build_bridge_customer_account_silverr'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
