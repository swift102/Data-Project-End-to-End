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

# # Silver Transform — Loan Participations
#  1. Notebook: `200_011_transform_loan_participations_silver`
#  2. Source: `lh_bronze_banking_data_modern_data.dbo.bronze_loan_participations` (221 rows)
#  3. Target: `lh_silver_banking_data.loan_participations`
#  4. Layer: Silver

# MARKDOWN ********************

# ## Imports and Configuration

# CELL ********************

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, DateType, DoubleType, BooleanType, IntegerType, TimestampType
)


START_TIME = datetime.datetime.utcnow()

config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)
MASK_SALT = config["MASK_SALT"]

SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_loan_participations"
TARGET_TABLE    = "loan_participations"   

PIPELINE_NAME = "200_011_transform_loan_participations_silver"
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

# ## Control Tables

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
    rows_inserted  BIGINT,
    rows_updated   BIGINT,
    status STRING,
    processed_timestamp TIMESTAMP
) USING DELTA
""")

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
) USING DELTA
""")

print("✅ Control tables ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Watermark

# CELL ********************

watermark_df = (
    spark.table("control.batch_watermark")
    .filter(F.col("pipeline_name") == PIPELINE_NAME)
    .filter(F.col("status") == "SUCCESS")
)

last_watermark = None
if watermark_df.count() > 0:
    last_watermark = watermark_df.agg(F.max("watermark_value")).collect()[0][0]

print("Last watermark:", last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze + CDC Dedup (Type 1)

# CELL ********************

bronze = spark.table(SOURCE_TABLE)

if last_watermark is not None:
    bronze = bronze.filter(F.col("_ingest_timestamp") > F.lit(last_watermark))
    print(f"Incremental — records after {last_watermark}")
else:
    print("First run — full load")

bronze_count = bronze.count()
print(f"Bronze rows loaded : {bronze_count:,}")


# Use _ingest_timestamp as the tie-breaker for latest record
w = Window.partitionBy("participation_id").orderBy(
    F.col("_ingest_timestamp").desc_nulls_last()
)

deduped = (
    bronze
    .withColumn("_row_rank", F.row_number().over(w))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank")   
)

deduped_count = deduped.count()
print(f"After CDC dedup    : {deduped_count:,}")
print(f"Rows resolved      : {bronze_count - deduped_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enforce Explicit Schema

# CELL ********************

typed = (
    deduped
    # Identity
    .withColumn("participation_id", F.trim(F.col("participation_id")).cast(StringType()))
    .withColumn("loan_id", F.trim(F.col("loan_id")).cast(StringType()))
    .withColumn("external_loan_reference", F.trim(F.col("external_loan_reference")).cast(StringType()))
    .withColumn("customer_id", F.trim(F.col("customer_id")).cast(StringType()))
    .withColumn("account_id", F.trim(F.col("account_id")).cast(StringType()))
    
    # Participation details
    .withColumn("participation_direction", F.trim(F.lower(F.col("participation_direction"))).cast(StringType()))
    .withColumn("originating_bank", F.trim(F.col("originating_bank")).cast(StringType()))
    .withColumn("servicing_bank", F.trim(F.col("servicing_bank")).cast(StringType()))
    .withColumn("participant_bank", F.trim(F.col("participant_bank")).cast(StringType()))
    .withColumn("participant_role", F.trim(F.lower(F.col("participant_role"))).cast(StringType()))
    .withColumn("loan_type", F.trim(F.col("loan_type")).cast(StringType()))
    
    # Dates & Metrics
    .withColumn("effective_date", F.col("effective_date").cast(DateType()))
    .withColumn("participation_pct", F.col("participation_pct").cast(DoubleType()))
    .withColumn("participation_amount", F.col("participation_amount").cast(DoubleType()))
    .withColumn("retained_pct", F.col("retained_pct").cast(DoubleType()))
    .withColumn("servicing_fee_bps", F.col("servicing_fee_bps").cast(DoubleType()))
    
    .withColumn("risk_share_type", F.trim(F.lower(F.col("risk_share_type"))).cast(StringType()))
    .withColumn("status", F.trim(F.lower(F.col("status"))).cast(StringType()))
    .withColumn("notes", F.trim(F.col("notes")).cast(StringType()))
    
    # No record_last_updated_at column in this Bronze table → removed
)

print("✅ Schema enforced")
print(f"Columns after typing : {len(typed.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Derived Columns

# CELL ********************

enriched = (
    typed
    .withColumn(
        "participation_bucket",
        F.when(F.col("participation_pct") < 25, "Low")
         .when(F.col("participation_pct") < 50, "Medium")
         .when(F.col("participation_pct") < 75, "High")
         .otherwise("Major")
    )
    .withColumn(
        "is_incoming_participation",
        F.col("participation_direction") == "incoming_participation"
    )
    .withColumn(
        "has_internal_loan",
        F.col("loan_id").isNotNull()
    )
    .withColumn(
        "is_external_loan",
        F.col("external_loan_reference").isNotNull()
    )
    .withColumn(
        "participation_source",
        F.when(F.col("participation_direction") == "incoming_participation", "External")
         .otherwise("Internal")
    )
    
    # Business columns
    .withColumn("participation_age_days",
        F.when(F.col("effective_date").isNotNull(),
               F.datediff(F.current_date(), F.col("effective_date")))
         .otherwise(None).cast(IntegerType())
    )
    .withColumn("is_active", F.col("status") == "active")
    .withColumn("retained_amount",
        F.col("participation_amount") * (F.col("retained_pct") / 100.0)
    )
    
    # Silver metadata
    .withColumn("record_source", F.lit("bronze_loan_participations"))
    .withColumn("created_timestamp", F.current_timestamp())
    .withColumn("updated_timestamp", F.current_timestamp())
    .withColumn("silver_batch_id", F.lit(SILVER_BATCH_ID))
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

# ## Write to Silver (Merge)

# CELL ********************

from delta.tables import DeltaTable

def merge_silver(df, table_name, business_key, total_rows):
    if not spark.catalog.tableExists(table_name):
        (df.write.format("delta").mode("overwrite").saveAsTable(table_name))
        print(f"✅ Created {table_name} (first run)")
        return total_rows, 0
    else:
        existing_keys = spark.table(table_name).select(business_key)
        inserts = df.join(existing_keys, business_key, "left_anti").count()
        updates = total_rows - inserts

        target = DeltaTable.forName(spark, table_name)
        update_set = {c: f"s.{c}" for c in df.columns if c != "created_timestamp"}
        update_set["updated_timestamp"] = "current_timestamp()"

        (target.alias("t")
         .merge(df.alias("s"), f"t.{business_key} = s.{business_key}")
         .whenMatchedUpdate(set=update_set)
         .whenNotMatchedInsertAll()
         .execute())
        
        print(f"✅ Merged into {table_name}")
        print(f"   Inserts : {inserts:,}")
        print(f"   Updates : {updates:,}")
        return inserts, updates

# Execute merge
enriched.cache()
rows_written = enriched.count()

rows_inserted, rows_updated = merge_silver(
    enriched, TARGET_TABLE, "participation_id", rows_written
)

print(f"\nRows processed : {rows_written:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Watermark + Audit Log

# CELL ********************

# Watermark
new_watermark = bronze.agg(F.max("_ingest_timestamp")).collect()[0][0]

spark.createDataFrame([{
    "pipeline_name": PIPELINE_NAME,
    "source_table": SOURCE_TABLE,
    "watermark_column": WATERMARK_COL,
    "watermark_value": new_watermark,
    "batch_id": SILVER_BATCH_ID,
    "rows_processed": bronze_count,
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "status": "SUCCESS",
    "processed_timestamp": datetime.datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable("control.batch_watermark")

# Audit Log
END_TIME = datetime.datetime.utcnow()
audit_record = spark.createDataFrame([(
    PIPELINE_NAME, SILVER_BATCH_ID, SOURCE_TABLE, rows_written,
    rows_inserted, rows_updated, START_TIME, END_TIME, "SUCCESS"
)], """
    pipeline_name STRING, batch_id STRING, source_table STRING,
    rows_processed BIGINT, rows_inserted BIGINT, rows_updated BIGINT,
    start_timestamp TIMESTAMP, end_timestamp TIMESTAMP, status STRING
""")

audit_record.write.format("delta").mode("append").saveAsTable("control.silver_audit_log")

print(f"✅ Watermark & Audit updated | Duration: {(END_TIME - START_TIME).total_seconds():.1f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Final Validation Summary

# CELL ********************

dim = spark.table(TARGET_TABLE)

print("="*70)
print("SILVER TRANSFORM SUMMARY — loan_participations")
print("="*70)
print(f"Batch ID : {SILVER_BATCH_ID}")
print(f"Rows     : {dim.count():,}\n")

dim.groupBy("participation_bucket").count().show()
dim.groupBy("participation_direction", "participation_source").count().show()
dim.groupBy("loan_type").count().show()
dim.groupBy("participant_bank").count().show()
dim.groupBy("is_active").count().show()

print("✅ Transformation completed successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
