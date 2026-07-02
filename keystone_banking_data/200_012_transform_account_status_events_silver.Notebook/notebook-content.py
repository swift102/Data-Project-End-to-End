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

# ## Configuration & Imports

# CELL ********************

import json, datetime
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, BooleanType, IntegerType, TimestampType
)

# Mask salt value (not used here - no PII in this table, kept for config consistency)
config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)
MASK_SALT = config["MASK_SALT"]


# Batch identity
SILVER_BATCH_ID  = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SILVER_LAYER     = "silver"
SOURCE_TABLE     = "lh_bronze_banking_data_modern_data.dbo.bronze_account_status_events"
TARGET_TABLE     = "account_status_events"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Source       : {SOURCE_TABLE}")
print(f"Target       : {TARGET_TABLE}")


# Watermark Metadata

PIPELINE_NAME  = "200_012_transform_account_status_events_silver"
WATERMARK_COL  = "_ingest_timestamp"

print(f"Pipeline     : {PIPELINE_NAME}")
print(f"Watermark col: {WATERMARK_COL}")


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

# ## Read Bronze (Incremental)

# CELL ********************

bronze = spark.table(SOURCE_TABLE)

if last_watermark is not None:
    bronze = bronze.filter(F.col(WATERMARK_COL) > F.lit(last_watermark))

incoming_count = bronze.count()
print(f"Incoming rows: {incoming_count:,}")

if incoming_count == 0:
    mssparkutils.notebook.exit("NO_NEW_DATA")

df = bronze


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema enforcement

# CELL ********************

status_events_schema = StructType([

    # Business keys
    StructField("account_id", StringType(), False),
    StructField("event_date", DateType(), False),
    StructField("event_type", StringType(), False),

    # Event details
    StructField("new_status", StringType(), True),
    StructField("status_reason", StringType(), True),

    # Derived columns
    StructField("is_risk_related", BooleanType(), True),
    StructField("is_closure_event", BooleanType(), True),
    StructField("event_sequence_on_account", IntegerType(), True),

    # Data quality flags
    StructField("is_orphan_account", BooleanType(), True),

    # Metadata
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

# ## Standardization and Validation

# CELL ********************

# Standardize text fields
df = (
    df
    .withColumn("event_type",
        F.lower(F.trim(F.col("event_type")))
    )
    .withColumn("new_status",
        F.lower(F.trim(F.col("new_status")))
    )
    .withColumn("status_reason",
        F.lower(F.trim(F.col("status_reason")))
    )
)

# Parse Event Date
df = df.withColumn(
    "event_date",
    F.coalesce(
        F.to_date("event_date", "yyyy-MM-dd"),
        F.to_date("event_date", "dd/MM/yyyy"),
        F.to_date("event_date", "MM/dd/yyyy")
    )
)

# Risk-Related Flag
df = df.withColumn(
    "is_risk_related",
    F.col("status_reason").isin(
        "high_risk_suspicion",
        "fraud_suspicion",
        "risk_monitoring",
        "moderate_risk"
    )
)

# Closure Event Flag
df = df.withColumn(
    "is_closure_event",
    F.col("new_status").isin("frozen", "suspended")
)

# Event Sequence Per Account (chronological order of events)
account_window = Window.partitionBy("account_id").orderBy("event_date")

df = df.withColumn(
    "event_sequence_on_account",
    F.row_number().over(account_window)
)

# Account Validation
accounts = spark.table("accounts")

df = (
    df.join(
        accounts.select("account_id").withColumn(
            "account_exists",
            F.lit(True)
        ),
        "account_id",
        "left"
    )
    .withColumn(
        "is_orphan_account",
        F.col("account_exists").isNull()
    )
    .drop("account_exists")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_df = df.select(
    "account_id",
    "event_date",
    "event_type",
    "new_status",
    "status_reason",
    "is_risk_related",
    "is_closure_event",
    "event_sequence_on_account",
    "is_orphan_account",
    "_source_file",
    "_ingest_timestamp",
    "_batch_id",
    "_commit_sha"
)

print(f"✅ Final schema applied — {len(final_df.columns)} columns")
final_df.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver Lakehouse

# CELL ********************

def merge_silver(df, table_name, business_keys):
    """
    business_keys: list of column names forming the composite natural key
    """

    if not spark.catalog.tableExists(table_name):

        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        inserts = df.count()
        updates = 0

        print(f"✅ Created {table_name}")
        print(f"Inserts: {inserts:,}")
        print(f"Updates: {updates:,}")

        return inserts, updates

    # Existing records
    existing_keys = spark.table(table_name).select(*business_keys)

    inserts = (
        df.join(existing_keys, business_keys, "left_anti").count()
    )

    updates = df.count() - inserts

    target = DeltaTable.forName(spark, table_name)

    merge_condition = " AND ".join(
        f"t.{k} = s.{k}" for k in business_keys
    )

    update_set = {c: f"s.{c}" for c in df.columns}

    (
        target.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged {table_name}")

    return inserts, updates


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute Delta Merge

BUSINESS_KEY = ["account_id", "event_date", "event_type"]

rows_inserted, rows_updated = merge_silver(
    final_df,
    TARGET_TABLE,
    BUSINESS_KEY
)

rows_written = final_df.count()

print(f"Rows Processed : {rows_written:,}")
print(f"Rows Inserted  : {rows_inserted:,}")
print(f"Rows Updated   : {rows_updated:,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Watermark

# CELL ********************

new_watermark = bronze.agg(F.max("_ingest_timestamp")).collect()[0][0]

spark.createDataFrame([{
    "pipeline_name":      PIPELINE_NAME,
    "source_table":       SOURCE_TABLE,
    "watermark_column":   WATERMARK_COL,
    "watermark_value":    new_watermark,
    "batch_id":           SILVER_BATCH_ID,
    "rows_processed":     bronze.count(),
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "status":             "SUCCESS",
    "processed_timestamp": datetime.datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable("control.batch_watermark")

print(f"✅ Watermark updated to {new_watermark}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Audit log

# CELL ********************

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

print("✅ Audit log updated")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Summary

# CELL ********************

dim = spark.table(TARGET_TABLE)

print("=" * 65)
print("  SILVER TRANSFORM SUMMARY — account_status_events")
print("=" * 65)
print(f"""
  Batch ID   : {SILVER_BATCH_ID}
  Source     : {SOURCE_TABLE}
  Target     : {TARGET_TABLE}
  Rows       : {dim.count():,}
""")

print("── Event type distribution ──")
dim.groupBy("event_type").count().orderBy("count", ascending=False).show()

print("── New status distribution ──")
dim.groupBy("new_status").count().orderBy("count", ascending=False).show()

print("── Status reason distribution ──")
dim.groupBy("status_reason").count().orderBy("count", ascending=False).show()

print("── Risk-related events ──")
dim.groupBy("is_risk_related").count().show()

print("── Orphan reference check ──")
print(f"  Orphan account_id  : {dim.filter(F.col('is_orphan_account') == True).count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
