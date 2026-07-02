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

# ## Imports and Config


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

df = spark.table("lh_bronze_banking_data_modern_data.dbo.bronze_collections_cases_collections_cases")

# Batch identity
SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

SOURCE_TABLE = "lh_bronze_banking_data_modern_data.dbo.bronze_collections_cases_collections_cases"
TARGET_TABLE = "collections_cases"

PIPELINE_NAME = "200_005_transform_collections_cases_silver"
WATERMARK_COL = "_ingest_timestamp"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Source       : {SOURCE_TABLE}")
print(f"Target       : {TARGET_TABLE}")

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

spark.sql("CREATE SCHEMA IF NOT EXISTS control")

spark.sql("""
CREATE TABLE IF NOT EXISTS control.batch_watermark (
    pipeline_name STRING,
    source_table STRING,
    watermark_column STRING,
    watermark_value TIMESTAMP,
    batch_id STRING,
    rows_processed BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    status STRING,
    processed_timestamp TIMESTAMP
)
USING DELTA
""")

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

last_watermark = None if watermark_df.count() == 0 else (
    watermark_df.agg(F.max("watermark_value")).collect()[0][0]
)

print("Last watermark:", last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality and Text Standardazition

# CELL ********************

df = df.withColumn("status", F.lower(F.trim(F.col("status")))) \
       .withColumn("collection_stage", F.lower(F.trim(F.col("collection_stage")))) \
       .withColumn("assigned_collector", F.trim(F.col("assigned_collector"))) \
       .withColumn("arrangement_plan", F.lower(F.trim(F.col("arrangement_plan"))))

# Standardise numeric 
df = df.withColumn(
    "arrears_amount",
    F.col("arrears_amount").cast("double")
).withColumn(
    "days_past_due",
    F.col("days_past_due").cast("int")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create derived columns
# 


# CELL ********************

df = df.withColumn(
    "is_ptp_case",
    F.col("promise_to_pay_amount").isNotNull()
)

df = df.withColumn(
    "is_arrangement_case",
    F.col("arrangement_plan").isNotNull()
)

df = df.withColumn(
    "arrears_bucket",
    F.when(F.col("arrears_amount") < 1000, "<1k")
     .when(F.col("arrears_amount") < 5000, "1k-5k")
     .when(F.col("arrears_amount") < 10000, "5k-10k")
     .otherwise("10k+")
)

df = df.withColumn(
    "dpd_bucket",
    F.when(F.col("days_past_due") <= 30, "1-30")
     .when(F.col("days_past_due") <= 60, "31-60")
     .when(F.col("days_past_due") <= 90, "61-90")
     .otherwise("90+")
)

df = df.withColumn(
    "stage_risk",
    F.when(F.col("collection_stage") == "pre_delinquent", "low")
     .when(F.col("collection_stage") == "early_collections", "medium")
     .when(F.col("collection_stage") == "late_collections", "high")
     .when(F.col("collection_stage") == "legal", "critical")
     .when(F.col("collection_stage") == "write_off", "loss")
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enforce Explicit Schema

# CELL ********************

collections_schema = StructType([
    StructField("case_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),

    StructField("arrears_amount", DoubleType(), True),
    StructField("days_past_due", IntegerType(), True),

    StructField("collection_stage", StringType(), True),
    StructField("status", StringType(), True),

    StructField("is_ptp_case", BooleanType(), True),
    StructField("is_arrangement_case", BooleanType(), True),

    StructField("arrears_bucket", StringType(), True),
    StructField("dpd_bucket", StringType(), True),
    StructField("stage_risk", StringType(), True),

    StructField("promise_to_pay_amount", DoubleType(), True),
    StructField("promise_to_pay_date", DateType(), True),
    StructField("arrangement_plan", StringType(), True),


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

# ## Write to SILVER LAYER


# CELL ********************

silver_fact = df.select(
    "case_id",
    "customer_id",
    "account_id",

    "arrears_amount",
    "days_past_due",

    "collection_stage",
    "status",
    "assigned_collector",

    "promise_to_pay_amount",
    "promise_to_pay_date",
    "arrangement_plan",

    "is_ptp_case",
    "is_arrangement_case",

    "arrears_bucket",
    "dpd_bucket",
    "stage_risk",
    
    "is_orphan_customer",
    "is_orphan_account"

    "_ingest_timestamp",
    "_batch_id"
)

(
    silver_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("lh_silver_banking_data.dbo.collections_cases")
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

rows_written = spark.table("lh_silver_banking_data.dbo.collections_cases").count()
rows_inserted = rows_written
rows_updated = 0

spark.createDataFrame([{
    "pipeline_name": PIPELINE_NAME,
    "source_table": SOURCE_TABLE,
    "watermark_column": WATERMARK_COL,
    "watermark_value": new_watermark,
    "batch_id": SILVER_BATCH_ID,
    "rows_processed": rows_processed,
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "status": "SUCCESS",
    "processed_timestamp": datetime.datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable("control.batch_watermark")

print(f"✅ Watermark updated: {new_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Audit Log


# CELL ********************

audit_record = spark.createDataFrame([(
    PIPELINE_NAME,
    SILVER_BATCH_ID,
    SOURCE_TABLE,
    rows_processed,
    rows_inserted,
    rows_updated,
    datetime.datetime.utcnow(),
    datetime.datetime.utcnow(),
    "SUCCESS"
)], """
pipeline_name STRING,
batch_id STRING,
source_table STRING,
rows_processed BIGINT,
rows_inserted BIGINT,
rows_updated BIGINT,
start_timestamp TIMESTAMP,
end_timestamp TIMESTAMP,
status STRING
""")

audit_record.write.format("delta").mode("append").saveAsTable("control.silver_audit_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Summary

# CELL ********************

silver = spark.table("lh_silver_banking_data.dbo.collections_cases")

print("=== SILVER VALIDATION SUMMARY ===")
print(f"Total rows              : {silver.count():,}")
print(f"Null case_id           : {silver.filter(F.col('case_id').isNull()).count():,}")
print(f"Orphan customers       : {silver.filter(F.col('is_orphan_customer')).count():,}")
print(f"Orphan accounts        : {silver.filter(F.col('is_orphan_account')).count():,}")

silver.groupBy("stage_risk").count().show()
silver.groupBy("arrears_bucket").count().show()
silver.groupBy("dpd_bucket").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
