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

# ## Import and Config

# CELL ********************

# ============================================================
# 200_016_transform_campaigns_silver
# Source : lh_bronze_banking_data_modern_data.dbo.bronze_marketing_campaigns_campaigns
# Target : lh_silver_banking_data.dbo.marketing_campaigns
# Strategy : Full overwrite — static snapshot, dedup on business cols
# ============================================================

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DateType, IntegerType, BooleanType
)

config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)

SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME   = "200_016_transform_campaigns_silver"
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_marketing_campaigns_campaigns"
TARGET_TABLE    = "marketing_campaigns"

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

# ##  Load Bronze — full load

# CELL ********************

bronze = spark.table(SOURCE_TABLE)
print(f"Bronze rows loaded : {bronze.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Deduplication

# CELL ********************

# Duplicates are byte-for-byte identical on business columns —
# same campaign row repeated across multiple monthly CSV files.
# dropDuplicates on business cols; keep first occurrence.

BUSINESS_COLS = [
    "campaign_id",
    "campaign_name",
    "campaign_type",
    "target_segment",
    "channel",
    "product_focus",
    "offer_summary",
    "start_date",
    "end_date",
    "budget_zar",
    "target_customers_count",
    "region",
    "status",
    "success_metric",
]

deduped = bronze.dropDuplicates(BUSINESS_COLS)
print(f"After dedup : {deduped.count():,}  (removed {bronze.count() - deduped.count():,} duplicates)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Enforcement + Transforms

# CELL ********************


REGION_MAP = {
    "Pretoria":     "Gauteng",
    "Johannesburg": "Gauteng",
    "Durban":       "KwaZulu-Natal",
}

region_map_expr = F.create_map(
    *[item for pair in
      [(F.lit(k), F.lit(v)) for k, v in REGION_MAP.items()]
      for item in pair]
)

typed = (
    deduped
    .withColumn("campaign_id",
        F.trim(F.col("campaign_id")).cast(StringType()))
    .withColumn("campaign_name",
        F.trim(F.col("campaign_name")).cast(StringType()))
    .withColumn("campaign_type",
        F.trim(F.lower(F.col("campaign_type"))).cast(StringType()))
    .withColumn("target_segment",
        F.trim(F.lower(F.col("target_segment"))).cast(StringType()))
    .withColumn("channel",
        F.trim(F.lower(F.col("channel"))).cast(StringType()))
    .withColumn("product_focus",
        F.trim(F.lower(F.col("product_focus"))).cast(StringType()))
    .withColumn("offer_summary",
        F.trim(F.col("offer_summary")).cast(StringType()))
    .withColumn("start_date",
        F.col("start_date").cast(DateType()))
    .withColumn("end_date",
        F.col("end_date").cast(DateType()))
    .withColumn("budget_zar",
        F.col("budget_zar").cast(IntegerType()))
    .withColumn("target_customers_count",
        F.col("target_customers_count").cast(IntegerType()))
    .withColumn("region",
        F.coalesce(
            region_map_expr[F.trim(F.col("region"))],
            F.trim(F.col("region"))
        ).cast(StringType()))
    .withColumn("status",
        F.trim(F.lower(F.col("status"))).cast(StringType()))
    .withColumn("success_metric",
        F.trim(F.lower(F.col("success_metric"))).cast(StringType()))
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
    .withColumn("campaign_sk",
        F.xxhash64(F.col("campaign_id")))

    # Duration
    .withColumn("duration_days",
        F.datediff(F.col("end_date"), F.col("start_date")))

    # Channel count — number of channels this execution used
    .withColumn("channel_count",
        F.size(F.split(F.col("channel"), ",")))

    # Active flag — based on dates, not status (status is always 'completed')
    .withColumn("is_active",
        F.current_date().between(F.col("start_date"), F.col("end_date")))

    # Audit
    .withColumn("record_source",       F.lit("bronze_marketing_campaigns_campaigns"))
    .withColumn("silver_batch_id",     F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp", F.current_timestamp())
    .withColumn("_ingest_timestamp",   F.col("_ingest_timestamp"))
    .withColumn("_batch_id",           F.col("_batch_id"))
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
    "campaign_sk",
    "campaign_id",

    # Descriptors
    "campaign_name",
    "campaign_type",
    "target_segment",
    "channel",
    "channel_count",
    "product_focus",
    "offer_summary",
    "region",
    "status",
    "success_metric",

    # Dates + duration
    "start_date",
    "end_date",
    "duration_days",

    # Financials
    "budget_zar",
    "target_customers_count",

    # Derived flags
    "is_active",

    # Audit
    "record_source",
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

# ## Write to Silver 

# CELL ********************


(
    silver.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(TARGET_TABLE)
)

rows_written = silver.count()
print(f"✅ Written to {TARGET_TABLE} : {rows_written:,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Control Tables

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType
)

watermark_schema = StructType([
    StructField("pipeline_name",       StringType(),    True),
    StructField("source_table",        StringType(),    True),
    StructField("watermark_column",    StringType(),    True),
    StructField("watermark_value",     TimestampType(), True),
    StructField("batch_id",            StringType(),    True),
    StructField("rows_processed",      LongType(),      True),
    StructField("rows_inserted",       LongType(),      True),
    StructField("rows_updated",        LongType(),      True),
    StructField("status",              StringType(),    True),
    StructField("processed_timestamp", TimestampType(), True),
])

(
    spark.createDataFrame([{
        "pipeline_name"       : PIPELINE_NAME,
        "source_table"        : SOURCE_TABLE,
        "watermark_column"    : None,
        "watermark_value"     : None,
        "batch_id"            : SILVER_BATCH_ID,
        "rows_processed"      : rows_written,
        "rows_inserted"       : rows_written,
        "rows_updated"        : 0,
        "status"              : "SUCCESS",
        "processed_timestamp" : datetime.datetime.utcnow(),
    }], schema=watermark_schema)
    .write.format("delta").mode("append")
    .saveAsTable("control.batch_watermark")
)

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
print("  SILVER TRANSFORM SUMMARY — marketing_campaigns")
print("=" * 60)
print(f"""
  Batch ID     : {SILVER_BATCH_ID}
  Source       : {SOURCE_TABLE}
  Target       : {TARGET_TABLE}

  Bronze rows loaded  : {bronze.count():,}
  Duplicates removed  : {bronze.count() - rows_written:,}
  Silver rows written : {rows_written:,}
""")

print("Campaign type distribution")
df.groupBy("campaign_type").count().orderBy("count", ascending=False).show()

print("Target segment distribution")
df.groupBy("target_segment").count().orderBy("count", ascending=False).show()

print("Product focus distribution")
df.groupBy("product_focus").count().orderBy("count", ascending=False).show()

print("Region distribution (post-normalisation)")
df.groupBy("region").count().orderBy("count", ascending=False).show()

print("Duration stats")
df.select(
    F.min("duration_days").alias("min_days"),
    F.max("duration_days").alias("max_days"),
    F.avg("duration_days").alias("avg_days")
).show()

print("=" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
