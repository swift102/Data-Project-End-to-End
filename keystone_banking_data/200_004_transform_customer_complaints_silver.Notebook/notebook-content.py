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
# **Notebook:** `200_004_transform_customer_communications_complaints_silver`  
# **Source:** `lh_bronze_banking_data.bronze_customer_communications_complaints` (8,825 rows)  
# **Target:** `lh_silver_banking_data.customer_complaints`  
# **Layer:** Silver  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Load Bronze complaints table |
# | 2 | Standardise categorical fields (`resolution_status`, `complaint_category`) |
# | 3 | Fix data quality issues (e.g. `escalted` → `escalated`) |
# | 4 | Parse and repair `timestamp` → `comm_ts` (multi-format fallback) |
# | 5 | Derive date attributes (`comm_date`, `year`, `month`, `day_of_week`) |
# | 6 | Validate referential integrity against customer & account dimensions |
# | 7 | Flag orphan records (`is_orphan_customer`, `is_orphan_account`) |
# | 8 | Clean text fields (`subject`, `body`) |
# | 9 | Create analytical feature (`customer_risk_band`) |
# | 10 | Write silver table + validation summary |
# 
# ---
# 
# ## Key Findings from Bronze Profiling
# 
# | Finding | Detail |
# |---|---|
# | Total records | 8,825 complaint records |
# | Sentiment | 100% `negative` (pre-classified dataset) |
# | Timestamp quality | ~54% (`4,760`) records have invalid/missing timestamps |
# | Referential integrity | 429 missing customers, 191 missing accounts |
# | Resolution status | `resolved` (5741), `open` (1994), `escalated` (1090) |
# | Category drivers | failed_transaction, service_delay, fee_dispute |
# | Channel distribution | phone_call, app_message, branch_visit dominate |
# | Direction | ~90% inbound complaints |
# | Data quality issues | Typo detected: `escalted` |
# | Risk signal | High-repeat customers identified (up to 9 complaints) |
# 
# ---


# MARKDOWN ********************

# ## Imports and Config


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, DateType, DoubleType,
    BooleanType, IntegerType, TimestampType
)

df = spark.table("lh_bronze_banking_data_modern_data.dbo.bronze_customer_communications_complaints")


# Batch identity 
SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_customer_communications_complaints"
TARGET_TABLE = "customer_complaints"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Source       : {SOURCE_TABLE}")
print(f"Target       : {TARGET_TABLE}")


# Watermark Metadata

PIPELINE_NAME = "200_004_transform_customer_communications_complaints_silver"
WATERMARK_COL = "_ingest_timestamp"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Watermark Table
#  
# Stores the last successfully processed _ingest_timestamp and other audit columns
# for each Silver table. On the first run the watermark is set to
# **'1970-01-01 00:00:00'** so all rows are processed.

# CELL ********************

spark.sql("""
CREATE SCHEMA IF NOT EXISTS control
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS control.batch_watermark (
    pipeline_name STRING,
    source_table STRING,
    watermark_column STRING,
    watermark_value TIMESTAMP,
    batch_id STRING,
    rows_processed BIGINT,
    rows_inserted  BIGINT,
    rows_updated   BIGINT,
    status STRING,
    processed_timestamp TIMESTAMP
)
USING DELTA
""")

print("✅ Watermark table is ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Audit log Table

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS control.silver_audit_log (
    pipeline_name STRING,
    batch_id STRING,
    source_table STRING,
    rows_processed BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING
)
USING DELTA
""")

print("✅ Audit log table is ready")

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

print("Last watermark:", last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enforce Explicit Schema

# CELL ********************

complaints_schema = StructType([

    # Identity
    StructField("comm_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),

    # Communication metadata
    StructField("channel", StringType(), True),
    StructField("direction", StringType(), True),

    # Time fields (IMPORTANT: cleaned version)
    StructField("comm_ts", TimestampType(), True),
    StructField("comm_date", DateType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),

    # Content
    StructField("subject", StringType(), True),
    StructField("body", StringType(), True),

    # Classification
    StructField("sentiment", StringType(), True),
    StructField("complaint_category", StringType(), True),
    StructField("resolution_status", StringType(), True),

    # Flags
    StructField("is_complaint", BooleanType(), True),
    StructField("is_orphan_customer", BooleanType(), True),
    StructField("is_orphan_account", BooleanType(), True),

    # Derived business feature
    StructField("customer_risk_band", StringType(), True),

    # Metadata (lineage / audit)
    StructField("_source_file", StringType(), True),
    StructField("_ingest_timestamp", TimestampType(), True),
    StructField("_batch_id", StringType(), True),
    StructField("_commit_sha", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality and Text Standardazition

# CELL ********************

df = df.withColumn(
    "resolution_status",
    F.lower(F.trim(F.col("resolution_status")))
)

# Correct spelling mistakes
df = df.withColumn(
    "resolution_status",
    F.when(F.col("resolution_status") == "escalted", "escalated")
     .otherwise(F.col("resolution_status"))
)


df = df.withColumn("subject", F.trim(F.col("subject"))) \
       .withColumn("body", F.trim(F.col("body")))



# Clean timestamp column
df = df.withColumn(
    "comm_ts",
    F.coalesce(

        # ISO8601 with timezone
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ssXXX"),

        # Standard datetime
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),

        # European format
        F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm"),

        # Slash format
        F.to_timestamp("timestamp", "yyyy/MM/dd HH:mm:ss"),

        # Month-name format
        F.to_timestamp("timestamp", "dd-MMM-yyyy HH:mm")

    )
)

print("Before parse nulls:", df.filter(F.to_timestamp("timestamp").isNull()).count())  # should be ~4,760
print("After parse nulls:",  df.filter(F.col("comm_ts").isNull()).count())              # should be 0


df = (
    df
    .withColumn("comm_date", F.to_date("comm_ts"))
    .withColumn("comm_year", F.year("comm_ts"))
    .withColumn("comm_month", F.month("comm_ts"))
    .withColumn("comm_quarter", F.quarter("comm_ts"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Referential Integrity Handling
# - Customer validation:
#   - `is_orphan_customer` = true when no match in customer dimension
# - Account validation:
#   - `is_orphan_account` = true when no match in account dimension


# CELL ********************

# Load the tables
customers = spark.table("customers_individual")
accounts = spark.table("accounts")

# Check if the customer exist
df = df.join(
    customers.select("customer_id"),
    on="customer_id",
    how="left"
).withColumn(
    "is_orphan_customer",
    F.col("customer_id").isNull()
)

# Check if the account exist
df = df.join(
    accounts.select("account_id"),
    on="account_id",
    how="left"
).withColumn(
    "is_orphan_account",
    F.col("account_id").isNull()
)

# Display the results
df.select(
    "customer_id",
    "account_id",
    "is_orphan_customer",
    "is_orphan_account"
).show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create derived risk features
# 1. complaint_risk_band
# 
# 
# 
# **Further risk features will be added upon request**

# CELL ********************

risk_df = df.groupBy("customer_id") \
    .agg(F.count("*").alias("complaint_count"))

df = df.join(risk_df, "customer_id", "left")

df = df.withColumn(
    "customer_risk_band",
    F.when(F.col("complaint_count") >= 8, "HIGH")
     .when(F.col("complaint_count") >= 5, "MEDIUM")
     .otherwise("LOW")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_validated = df.select(
    [F.col(f.name).cast(f.dataType).alias(f.name) for f in complaints_schema]
)

rows_inserted = df_validated.count()
rows_updated  = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to SILVER LAYER


# CELL ********************

# Load silver columns
silver_fact = df.select(
    "comm_id",
    "customer_id",
    "account_id",
    "channel",
    "direction",
    "body",
    "comm_ts",
    "comm_date",
    "year",
    "month",
    F.date_format("comm_ts", "EEEE").alias("day_of_week"), 
    "subject",
    "sentiment",
    "complaint_category",
    "resolution_status",
    "is_complaint",
    "customer_risk_band",
    "is_orphan_customer",
    "is_orphan_account",
    "_ingest_timestamp",
    "_batch_id"
)

(
    silver_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_silver_banking_data.dbo.customer_complaints")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Watermark

# CELL ********************

new_watermark = df.agg(F.max("_ingest_timestamp")).collect()[0][0]

rows_processed = df.count() 

spark.createDataFrame([{
    "pipeline_name":       PIPELINE_NAME,
    "source_table":        SOURCE_TABLE,
    "watermark_column":    WATERMARK_COL,
    "watermark_value":     new_watermark,
    "batch_id":            SILVER_BATCH_ID,
    "rows_processed":      rows_processed,
    "rows_inserted":       rows_inserted,
    "rows_updated":        rows_updated,
    "status":              "SUCCESS",
    "processed_timestamp": datetime.datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable("control.batch_watermark")

print(f"✅ Watermark updated to {new_watermark}")
print(f"   Rows processed: {rows_processed:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Audit Log


# CELL ********************

rows_written = spark.table("lh_silver_banking_data.dbo.customer_complaints").count()
rows_inserted = rows_written  # full overwrite, every row is an insert
rows_updated  = 0

audit_record = spark.createDataFrame(
    [(
        PIPELINE_NAME,
        SILVER_BATCH_ID,
        SOURCE_TABLE,
        rows_written,
        rows_inserted,
        rows_updated,
        datetime.datetime.utcnow(),
        datetime.datetime.utcnow(),
        "SUCCESS"
    )],
    """
    pipeline_name STRING,
    batch_id STRING,
    source_table STRING,
    rows_processed BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING
    """
)

(
    audit_record.write
        .format("delta")
        .mode("append")
        .saveAsTable("control.silver_audit_log")
)

print(f"✅ Audit log updated — {rows_written:,} rows recorded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Summary

# CELL ********************

silver = spark.table("lh_silver_banking_data.dbo.customer_complaints")

print("=== SILVER VALIDATION SUMMARY ===")
print(f"Total rows written      : {silver.count():,}")
print(f"Null comm_ts            : {silver.filter(F.col('comm_ts').isNull()).count():,}")
print(f"Orphan customers        : {silver.filter(F.col('is_orphan_customer')).count():,}")
print(f"Orphan accounts         : {silver.filter(F.col('is_orphan_account')).count():,}")
print(f"Resolution status values: {[r[0] for r in silver.select('resolution_status').distinct().collect()]}")
print(f"Risk band distribution  :")
silver.groupBy("customer_risk_band").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
