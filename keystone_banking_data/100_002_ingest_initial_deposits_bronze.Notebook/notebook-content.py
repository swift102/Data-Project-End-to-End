# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e1b0fd50-8d63-4667-998b-3fd590fa7ff9",
# META       "default_lakehouse_name": "lh_bronze_banking_data_modern_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "e1b0fd50-8d63-4667-998b-3fd590fa7ff9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze Ingest — `initial_deposits/**/*.jsonl` → `bronze.bronze_initial_deposits`
# 
# **Source:** `Files/bronze_raw/banking_data/initial_deposits/**/*.jsonl`
# **Target:** `bronze.bronze_initial_deposits` (Delta)
# 
# **Context:** `initial_deposits` files were absent from the source repository at the time
# of the original Bronze ingest (`100_001`, batch `20260611T154040Z`). The files were
# subsequently added to the repo and landed in `bronze_raw` during a later reingest,
# but no Delta table was created for them.
# 
# **Strategy:** One-shot backfill. Source is a static partitioned JSONL dataset — one file
# per month/year, each record representing the opening deposit for a given account on its
# activation date. Written with `OVERWRITE` mode; safe to rerun.
# 
# **Scope:**
# - Reads all JSONL files recursively under `initial_deposits/`
# - Carries `year` and `month` partition values from the file path
# - Flattens `channel_metadata` struct into scalar columns
# - Adds `transaction_type = 'INITIAL_DEPOSIT'` as a structural marker
# - Adds standard Bronze audit columns (`_source_file`, `_batch_id`, `_ingest_timestamp`)
# - No synthetic keys, no DQ transforms — Bronze stays faithful to source
# 
# > **Downstream:** Silver `200_008` unions this table into `silver_transactions` before merge.
# > Opening balances will be absent from `fact_transaction` until `200_008` is rerun post this backfill.


# CELL ********************

import json, datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    DateType, StructType
)

config_json = mssparkutils.notebook.run("000_Config", 60, {"useRootDefaultLakehouse": True})
config      = json.loads(config_json)

TABLE_PREFIX = config.get("bronze_schema", "")
BATCH_ID     = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SOURCE_PATH = "Files/bronze_raw/banking_data/*/*/initial_deposits.jsonl"
TARGET_TABLE = "bronze_initial_deposits"

def tbl(name): return f"{TABLE_PREFIX}.{name}" if TABLE_PREFIX else name

print(f"Batch    : {BATCH_ID}")
print(f"Source   : {SOURCE_PATH}")
print(f"Target   : {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read JSONL
raw = (
    spark.read
         .option("multiLine", False)
         .json(SOURCE_PATH)
)

print(f"Raw row count : {raw.count():,}")
raw.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Flatten channel_metadata + add Bronze audit columns 
df = (
    raw
    .select(
        F.col("account_id"),
        F.col("amount").cast(DoubleType()),
        F.col("channel"),
        F.col("channel_metadata.branch_code").alias("branch_code"),
        F.col("channel_metadata.terminal_id").alias("terminal_id"),
        F.col("channel_metadata.atm_id").alias("atm_id"),
        F.col("transaction_date").cast(DateType()),
    )
    .withColumn("transaction_type",  F.lit("INITIAL_DEPOSIT"))
    .withColumn("_source_file",      F.lit("initial_deposits.jsonl"))
    .withColumn("_batch_id",         F.lit(BATCH_ID))
    .withColumn("_ingest_timestamp", F.current_timestamp())
)

print(f"Final row count : {df.count():,}")
df.printSchema()
df.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write Bronze Delta (OVERWRITE — source is static)
(
    df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(TARGET_TABLE)
)

print(f"✅ Written to {TARGET_TABLE}")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {TARGET_TABLE}").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
