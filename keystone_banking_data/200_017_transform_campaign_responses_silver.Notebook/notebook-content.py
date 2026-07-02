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

# ## Import and config

# CELL ********************

# ============================================================
# 200_017_transform_campaign_responses_silver
# Source : lh_bronze_banking_data_modern_data.dbo.bronze_marketing_campaigns_campaign_responses
# Target : lh_silver_banking_data.dbo.marketing_campaign_responses
# Strategy : Incremental merge on response_id (fully unique natural key)
# ============================================================

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, DateType, DoubleType, BooleanType, TimestampType
)

config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)

SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME   = "200_017_transform_campaign_responses_silver"
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_marketing_campaigns_campaign_responses"
TARGET_TABLE    = "marketing_campaign_responses"
WATERMARK_COL   = "_ingest_timestamp"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Pipeline     : {PIPELINE_NAME}")
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

# ## Read Watermark

# CELL ********************

wm_row = (
    spark.table("control.batch_watermark")
         .filter(F.col("pipeline_name") == PIPELINE_NAME)
         .orderBy(F.col("processed_timestamp").desc())
         .limit(1)
         .collect()
)

last_watermark = wm_row[0]["watermark_value"] if wm_row else None
print(f"Last watermark : {last_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze — incremental on _ingest_timestamp

# CELL ********************

bronze = spark.table(SOURCE_TABLE)

if last_watermark:
    bronze = bronze.filter(F.col(WATERMARK_COL) > last_watermark)

new_watermark = bronze.agg(F.max(WATERMARK_COL)).collect()[0][0]
print(f"Bronze rows loaded : {bronze.count():,}")
print(f"New watermark      : {new_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Enforcement + Transforms

# CELL ********************

typed = (
    bronze
    .withColumn("response_id",
        F.trim(F.col("response_id")).cast(StringType()))
    .withColumn("campaign_id",
        F.trim(F.col("campaign_id")).cast(StringType()))
    .withColumn("customer_id",
        F.trim(F.col("customer_id")).cast(StringType()))
    .withColumn("account_id",
        F.trim(F.col("account_id")).cast(StringType()))
    .withColumn("response_date",
        F.col("response_date").cast(DateType()))
    .withColumn("response_type",
        F.trim(F.lower(F.col("response_type"))).cast(StringType()))
    .withColumn("conversion_value_zar",
        F.col("conversion_value_zar").cast(DoubleType()))
    .withColumn("channel_used",
        F.trim(F.lower(F.col("channel_used"))).cast(StringType()))
)

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
    # Surrogate key
    .withColumn("response_sk",
        F.xxhash64(F.col("response_id")))

    # Engagement classification
    .withColumn("is_positive_response",
        F.col("response_type").isin("opened", "clicked", "converted").cast(BooleanType()))
    .withColumn("is_negative_response",
        F.col("response_type").isin("opted_out", "complained").cast(BooleanType()))
    .withColumn("is_converted",
        (F.col("response_type") == "converted").cast(BooleanType()))
    .withColumn("is_opted_out",
        (F.col("response_type") == "opted_out").cast(BooleanType()))

    # Temporal
    .withColumn("response_year",
        F.year(F.col("response_date")).cast(StringType()))
    .withColumn("response_month",
        F.month(F.col("response_date")).cast(StringType()))

    # Audit
    .withColumn("record_source",         F.lit("bronze_marketing_campaigns_campaign_responses"))
    .withColumn("created_timestamp",     F.current_timestamp())
    .withColumn("silver_batch_id",       F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp", F.current_timestamp())
    .withColumn("_ingest_timestamp",     F.col("_ingest_timestamp"))
    .withColumn("_batch_id",             F.col("_batch_id"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Column Selection

# CELL ********************

SILVER_COLS = [
    # Keys
    "response_sk",
    "response_id",
    "campaign_id",
    "customer_id",
    "account_id",

    # Response
    "response_date",
    "response_year",
    "response_month",
    "response_type",
    "channel_used",

    # Financials
    "conversion_value_zar",

    # Derived flags
    "is_positive_response",
    "is_negative_response",
    "is_converted",
    "is_opted_out",

    # Audit
    "record_source",
    "created_timestamp",
    "silver_batch_id",
    "silver_load_timestamp",
    "_ingest_timestamp",
    "_batch_id",
]

silver = enriched.select(SILVER_COLS)
print(f"✅ Column selection applied : {len(silver.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver — Delta Merge on response_sk

# CELL ********************

from delta.tables import DeltaTable

def merge_silver(df, table_name, merge_key):
    if not spark.catalog.tableExists(table_name):
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        inserts = df.count()
        updates = 0
        print(f"✅ Created {table_name} : {inserts:,} rows")
        return inserts, updates

    update_set = {
        c: f"s.{c}"
        for c in df.columns
        if c != "created_timestamp"
    }
    update_set["silver_load_timestamp"] = "current_timestamp()"

    (
        DeltaTable.forName(spark, table_name)
        .alias("t")
        .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    metrics = (
        DeltaTable.forName(spark, table_name)
        .history(1)
        .select(
            F.col("operationMetrics.numTargetRowsInserted").cast("long").alias("inserted"),
            F.col("operationMetrics.numTargetRowsUpdated").cast("long").alias("updated"),
        )
        .collect()[0]
    )
    inserts = metrics["inserted"] or 0
    updates = metrics["updated"] or 0
    print(f"✅ Merged {table_name}")
    print(f"   Inserts : {inserts:,}")
    print(f"   Updates : {updates:,}")
    return inserts, updates

rows_inserted, rows_updated = merge_silver(silver, TARGET_TABLE, "response_sk")
rows_written = rows_inserted + rows_updated

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Watermark

# CELL ********************

(
    spark.createDataFrame([{
        "pipeline_name"       : PIPELINE_NAME,
        "source_table"        : SOURCE_TABLE,
        "watermark_column"    : WATERMARK_COL,
        "watermark_value"     : new_watermark,
        "batch_id"            : SILVER_BATCH_ID,
        "rows_processed"      : bronze.count(),
        "rows_inserted"       : rows_inserted,
        "rows_updated"        : rows_updated,
        "status"              : "SUCCESS",
        "processed_timestamp" : datetime.datetime.utcnow(),
    }])
    .write.format("delta").mode("append")
    .saveAsTable("control.batch_watermark")
)

(
    spark.createDataFrame([{
        "pipeline_name"   : PIPELINE_NAME,
        "batch_id"        : SILVER_BATCH_ID,
        "source_table"    : SOURCE_TABLE,
        "rows_processed"  : bronze.count(),
        "rows_inserted"   : rows_inserted,
        "rows_updated"    : rows_updated,
        "start_timestamp" : datetime.datetime.utcnow(),
        "end_timestamp"   : datetime.datetime.utcnow(),
        "status"          : "SUCCESS",
    }])
    .write.format("delta").mode("append")
    .saveAsTable("control.silver_audit_log")
)

print("✅ Control tables updated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Summary

# CELL ********************

df = spark.table(TARGET_TABLE)

print("=" * 60)
print("  SILVER TRANSFORM SUMMARY — marketing_campaign_responses")
print("=" * 60)
print(f"""
  Batch ID     : {SILVER_BATCH_ID}
  Source       : {SOURCE_TABLE}
  Target       : {TARGET_TABLE}

  Bronze rows loaded  : {bronze.count():,}
  Rows inserted       : {rows_inserted:,}
  Rows updated        : {rows_updated:,}

  Watermark
  Previous : {last_watermark}
  New      : {new_watermark}
""")

print("Response type distribution")
df.groupBy("response_type").count().orderBy("count", ascending=False).show()

print("Channel used distribution")
df.groupBy("channel_used").count().orderBy("count", ascending=False).show()

print("Engagement flags")
print(f"  is_positive_response : {df.filter(F.col('is_positive_response')).count():,}")
print(f"  is_negative_response : {df.filter(F.col('is_negative_response')).count():,}")
print(f"  is_converted         : {df.filter(F.col('is_converted')).count():,}")
print(f"  is_opted_out         : {df.filter(F.col('is_opted_out')).count():,}")

print("\nOpted-in customers (positive, no opt-out)")
opted_in = (
    df.groupBy("customer_id")
      .agg(
          F.max(F.col("is_positive_response").cast("int")).alias("has_engaged"),
          F.max(F.col("is_negative_response").cast("int")).alias("has_opted_out"),
      )
      .withColumn("is_opted_in_marketing",
          (F.col("has_engaged") == 1) & (F.col("has_opted_out") == 0))
      .groupBy("is_opted_in_marketing").count()
)
opted_in.show()

print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
