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

# # Silver Transform — Debit Orders
# 
# **Notebook:** `200_009_transform_debit_orders_silver`  
# **Source:** `lh_bronze_banking_data_modern_data.dbo.bronze_debit_orders` (399,898 rows)  
# **Target:** `lh_silver_banking_data.debit_orders`  
# **Layer:** Silver  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |------|-----------|
# | 1 | Load Bronze + deduplication on (debit_order_id, record_last_updated_at) |
# | 2 | Standardize dates (especially suspension_date) |
# | 3 | Enforce explicit schema + cleaning |
# | 4 | PII masking on beneficiary_name |
# | 5 | Derived columns & flags |
# | 6 | Write to Silver with Delta Merge + watermarking |
# | 7 | Validation summary |
# 
# ---
# 
# ## Key Findings from Bronze Profiling
# 
# - ~400k rows, 91.9k distinct debit_order_id → historical versions
# - 87% Loan Repayment
# - Strong linkage to loans (344k linked)
# - suspension_date is string → standardize

# MARKDOWN ********************

# ## Configuration & Imports

# CELL ********************

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

config = json.loads(notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True}))
MASK_SALT = config["MASK_SALT"]

SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME   = "200_009_transform_debit_orders_silver"
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_debit_orders"  
TARGET_TABLE    = "debit_orders"
WATERMARK_COL   = "_ingest_timestamp"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Pipeline     : {PIPELINE_NAME}")
print(f"Source       : {SOURCE_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Control Tables

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS control")

spark.sql("""
CREATE TABLE IF NOT EXISTS control.batch_watermark (
    pipeline_name       STRING,
    source_table        STRING,
    watermark_column    STRING,
    watermark_value     TIMESTAMP,
    batch_id            STRING,
    rows_processed      BIGINT,
    rows_inserted       BIGINT,
    rows_updated        BIGINT,
    status              STRING,
    processed_timestamp TIMESTAMP
)
USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS control.silver_audit_log (
    pipeline_name    STRING,
    batch_id         STRING,
    source_table     STRING,
    rows_processed   BIGINT,
    rows_inserted    BIGINT,
    rows_updated     BIGINT,
    start_timestamp  TIMESTAMP,
    end_timestamp    TIMESTAMP,
    status           STRING
)
USING DELTA
""")

print("✅ Control tables ready")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Existing Watermark


# CELL ********************

watermark_df = (
    spark.table("control.batch_watermark")
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .filter(F.col("status") == "SUCCESS")
)

if watermark_df.count() == 0:
    last_watermark = None
else:
    last_watermark = (
        watermark_df
        .agg(F.max("watermark_value"))
        .collect()[0][0]
    )

print(f"Last watermark : {last_watermark}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze & Deduplication

# CELL ********************

bronze_full = spark.table(SOURCE_TABLE)

if last_watermark is None:
    bronze = bronze_full
    print("First run — full load")
else:
    bronze = bronze_full.filter(F.col(WATERMARK_COL) > F.lit(last_watermark))
    print(f"Incremental — records after {last_watermark}")

print(f"Bronze rows loaded : {bronze.count():,}")

# ONLY remove true duplicate rows (same ingest)
w = Window.partitionBy("debit_order_id").orderBy(
    F.desc("year"), F.desc("month")
)

deduped = (
    bronze
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"After dedup: {deduped.count():,}")
print(f"Duplicates removed : {bronze.count() - deduped.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enforce Explicit Schema + Cleaning

# CELL ********************

typed = (
    deduped
    # Core identifiers
    .withColumn("debit_order_id", F.trim(F.col("debit_order_id")).cast(StringType()))
    .withColumn("account_id", F.trim(F.col("account_id")).cast(StringType()))
    .withColumn("customer_id", F.trim(F.col("customer_id")).cast(StringType()))
    .withColumn("creditor_id", F.trim(F.col("creditor_id")).cast(StringType()))

    # Dates — standardize all to timestamp
    .withColumn("start_date", F.to_timestamp(F.col("start_date")))
    .withColumn("end_date", F.to_timestamp(F.col("end_date")))
    .withColumn("cancellation_date", F.to_timestamp(F.col("cancellation_date")))
    .withColumn("suspension_date",
                F.when(F.col("suspension_date").isNotNull(), F.to_timestamp(F.col("suspension_date")))
                 .otherwise(F.lit(None).cast(TimestampType())))
    .withColumn("record_last_updated_at", F.to_timestamp(F.col("record_last_updated_at")))

    # Numeric
    .withColumn("amount", F.col("amount").cast(DoubleType()))
    .withColumn("collection_day", F.col("collection_day").cast(IntegerType()))
    .withColumn("notification_days_before", F.col("notification_days_before").cast(IntegerType()))

    # Categoricals + trim
    .withColumn("debit_order_type", F.trim(F.col("debit_order_type")).cast(StringType()))
    .withColumn("frequency", F.trim(F.col("frequency")).cast(StringType()))
    .withColumn("status", F.trim(F.lower(F.col("status"))).cast(StringType()))
    .withColumn("suspension_reason", F.trim(F.col("suspension_reason")).cast(StringType()))
    .withColumn("cancellation_reason", F.trim(F.col("cancellation_reason")).cast(StringType()))
    .withColumn("notification_method", F.trim(F.col("notification_method")).cast(StringType()))
    .withColumn("beneficiary_name", F.trim(F.col("beneficiary_name")).cast(StringType()))
    .withColumn("description", F.trim(F.col("description")).cast(StringType()))

    # Booleans
    .withColumn("is_fixed_amount", F.col("is_fixed_amount").cast(BooleanType()))
    .withColumn("can_be_reactivated", F.col("can_be_reactivated").cast(BooleanType()))
    .withColumn("notification_required", F.col("notification_required").cast(BooleanType()))

    # Linked references
    .withColumn("linked_loan_id", F.trim(F.col("linked_loan_id")).cast(StringType()))
    .withColumn("linked_policy_number", F.trim(F.col("linked_policy_number")).cast(StringType()))
    .withColumn("linked_subscription_id", F.trim(F.col("linked_subscription_id")).cast(StringType()))
)

print("✅ Schema enforced")
print(f"Columns after typing : {len(typed.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PII Masking

# CELL ********************

masked = (
    typed
    .withColumn("beneficiary_name",
        F.when(
            F.col("beneficiary_name").isNotNull(),
            F.sha2(F.concat(F.col("beneficiary_name"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )
)

print("✅ PII masking applied")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Derived Columns

# CELL ********************

enriched = (
    masked
    # Surrogate Key — single row per debit_order_id
    .withColumn(
        "debit_order_sk",
        F.xxhash64(F.col("debit_order_id"))
    )

    # Status flags
    .withColumn("is_active",    F.col("status") == "active")
    .withColumn("is_suspended", F.col("status") == "suspended")
    .withColumn("is_cancelled", F.col("status") == "cancelled")

    # Other flags
    .withColumn("is_recurring",         F.col("frequency").isin("Monthly", "Weekly", "Quarterly"))
    .withColumn("is_linked_to_loan",    F.col("linked_loan_id").isNotNull())
    .withColumn("is_fixed",             F.col("is_fixed_amount") == True)
    .withColumn("notification_required", F.coalesce(F.col("notification_required"), F.lit(False)))

    # Time components — derived from record_last_updated_at (the natural anchor)
    .withColumn("effective_year",  F.year(F.col("record_last_updated_at")).cast(IntegerType()))
    .withColumn("effective_month", F.month(F.col("record_last_updated_at")).cast(IntegerType()))

    # Audit
    .withColumn("record_source",        F.lit("bronze_debit_orders"))
    .withColumn("created_timestamp",    F.current_timestamp())
    .withColumn("updated_timestamp",    F.current_timestamp())
    .withColumn("silver_batch_id",      F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp", F.current_timestamp())
)

print("✅ Derived columns added")
print(f"Total columns : {len(enriched.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Final Column Selection

# CELL ********************

SILVER_COLS = [
    # Identity
    "debit_order_sk", "debit_order_id",
    "account_id", "customer_id",

    # Core attributes
    "debit_order_type", "amount", "frequency",
    "collection_day", "is_fixed_amount",
    "start_date", "end_date",

    # Status
    "status", "is_active", "is_suspended", "is_cancelled", "is_recurring",

    # Suspension
    "suspension_date", "suspension_reason", "suspension_initiated_by",

    # Cancellation
    "cancellation_date", "cancellation_reason", "can_be_reactivated",

    # Notification
    "notification_required", "notification_days_before", "notification_method",

    # Beneficiary
    "beneficiary_name", "beneficiary_account_number", "beneficiary_branch_code",
    "beneficiary_bank_name", "beneficiary_account_type",

    # Linkages
    "creditor_id", "linked_loan_id", "is_linked_to_loan",
    "linked_policy_number", "linked_subscription_id", "description",

    # Temporal
    "record_last_updated_at", "effective_year", "effective_month",

    # Audit / lineage
    "record_source", "created_timestamp", "updated_timestamp",
    "silver_batch_id", "silver_load_timestamp",
    "_source_file", "_ingest_timestamp",
]

silver = enriched.select([c for c in SILVER_COLS if c in enriched.columns])
print(f"Final schema: {len(silver.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver — Delta Merge

# CELL ********************

from delta.tables import DeltaTable

def merge_silver(df, table_name):

    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"✅ Table {table_name} created (first load)")
        n = df.count()
        return n, 0

    target = DeltaTable.forName(spark, table_name)

    (
        target.alias("t")
        .merge(df.alias("s"), "t.debit_order_id = s.debit_order_id")
        .whenMatchedUpdateAll()   # overwrites all cols if record already exists
        .whenNotMatchedInsertAll() # inserts new debit orders
        .execute()
    )

    n = df.count()
    print(f"✅ Silver merge complete — {n:,} rows processed")
    return n, n  # inserted + updated 

rows_inserted, rows_updated = merge_silver(silver, TARGET_TABLE)
rows_written = silver.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Watermark 

# CELL ********************

new_watermark = bronze.agg(F.max("_ingest_timestamp")).collect()[0][0]

(spark.createDataFrame([{
    "pipeline_name": PIPELINE_NAME,
    "source_table": SOURCE_TABLE,
    "watermark_column": WATERMARK_COL,
    "watermark_value": new_watermark,
    "batch_id": SILVER_BATCH_ID,
    "rows_processed": rows_written,
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "status": "SUCCESS",
    "processed_timestamp": datetime.datetime.utcnow(),
}]).write.format("delta").mode("append").saveAsTable("control.batch_watermark"))

print(f"✅ Watermark updated to {new_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Audit Log

# CELL ********************

(spark.createDataFrame([{
    "pipeline_name": PIPELINE_NAME,
    "batch_id": SILVER_BATCH_ID,
    "source_table": SOURCE_TABLE,
    "rows_processed": rows_written,
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "start_timestamp": datetime.datetime.utcnow(),
    "end_timestamp": datetime.datetime.utcnow(),
    "status": "SUCCESS",
}]).write.format("delta").mode("append").saveAsTable("control.silver_audit_log"))

print("✅ Audit log updated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Summary

# CELL ********************

do = spark.table(TARGET_TABLE)

print("=" * 75)
print("  SILVER TRANSFORM SUMMARY — debit_orders")
print("=" * 75)
print(f"""
  Batch ID              : {SILVER_BATCH_ID}
  Source                : {SOURCE_TABLE}
  Target                : {TARGET_TABLE}
  Rows Written          : {rows_written:,}
  Distinct Debit Orders : {do.select('debit_order_id').distinct().count():,}
""")

print("\nStatus distribution")
do.groupBy("status").count().orderBy(F.desc("count")).show()

print("\nDebit Order Type (Top 10)")
do.groupBy("debit_order_type").count().orderBy(F.desc("count")).show(10)

print("\nFrequency")
do.groupBy("frequency").count().orderBy(F.desc("count")).show()

print(f"\nLinked to loans  : {do.filter(F.col('is_linked_to_loan')).count():,}")
print(f"Active mandates  : {do.filter(F.col('is_active')).count():,}")
print(f"Recurring orders : {do.filter(F.col('is_recurring')).count():,}")

print("\n" + "=" * 75)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_silver_banking_data.control.batch_watermark LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_silver_banking_data.control.silver_audit_log LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BATCH_ID = "20260616T154025Z"
PIPELINE_NAME = "200_009_transform_debit_orders_silver"

spark.sql(f"""
SELECT *
FROM control.silver_audit_log
WHERE batch_id = '{BATCH_ID}'
AND pipeline_name = '{PIPELINE_NAME}'
""").show(truncate=False)


spark.sql(f"""
SELECT *
FROM control.batch_watermark
WHERE batch_id = '{BATCH_ID}'
AND pipeline_name = '{PIPELINE_NAME}'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

BATCH_ID = "20260616T205954Z"
PIPELINE_NAME = "200_009_transform_debit_orders_silver"


spark.sql(f"""
DELETE FROM control.silver_audit_log
WHERE batch_id = '{BATCH_ID}'
AND pipeline_name = '{PIPELINE_NAME}'
""")


spark.sql(f"""
DELETE FROM control.batch_watermark
WHERE batch_id = '{BATCH_ID}'
AND pipeline_name = '{PIPELINE_NAME}'
""")

print("✅ Deleted Silver batch control records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
