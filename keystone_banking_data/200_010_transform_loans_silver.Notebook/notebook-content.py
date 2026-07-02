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

# # Silver Transform — Loans
# 
# **Notebook:** `200_010_transform_loans_silver`  
# **Source:** `bronze_loans` (110,894 rows)  
# **Target:** `lh_silver_banking_data.loans`  
# **Layer:** Silver
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |------|-----------|
# | 1 | Load Bronze + deduplication on `loan_id` |
# | 2 | Enforce explicit schema + type fixes |
# | 3 | Null classification & business rule fixes |
# | 4 | Derived business columns (`loan_status`, `is_booked`, etc.) |
# | 5 | Fix data quality issues (`discretionary_income` < 0, etc.) |
# | 6 | Date enrichment + partitioning columns |
# | 7 | Write to Silver with Delta merge + audit |

# CELL ********************

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DateType, DoubleType, BooleanType, IntegerType, TimestampType, LongType
)
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# Config
config = json.loads(notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True}))
MASK_SALT = config["MASK_SALT"]

SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SOURCE_TABLE = "lh_bronze_banking_data_modern_data.dbo.bronze_loans"
TARGET_TABLE = "loans"

PIPELINE_NAME = "200_010_transform_loans_silver"
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

watermark_df = spark.table("control.batch_watermark") \
    .filter(F.col("pipeline_name") == PIPELINE_NAME) \
    .filter(F.col("status") == "SUCCESS")

last_watermark = None if watermark_df.count() == 0 else \
    watermark_df.agg(F.max("watermark_value")).collect()[0][0]

print("Last watermark:", last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze + Dedup

# CELL ********************

bronze = spark.table(SOURCE_TABLE)

if last_watermark is not None:
    bronze = bronze.filter(F.col("_ingest_timestamp") > F.lit(last_watermark))
    print("Incremental load")
else:
    print("Full load")

print(f"Bronze rows: {bronze.count():,}")

# Dedup on loan_id (should already be unique)
w = Window.partitionBy("loan_id").orderBy(F.col("_ingest_timestamp").desc())
deduped = bronze.withColumn("_rn", F.row_number().over(w)) \
    .filter(F.col("_rn") == 1).drop("_rn")

print(f"After dedup: {deduped.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enforce Schema + Standardization

# CELL ********************

silver = (
    deduped
    .withColumn("loan_id", F.trim(F.col("loan_id")).cast(StringType()))
    .withColumn("customer_id", F.trim(F.col("customer_id")).cast(StringType()))
    .withColumn("account_id", F.trim(F.col("account_id")).cast(StringType()))
    .withColumn("loan_type", F.trim(F.col("loan_type")).cast(StringType()))
    .withColumn("application_status", F.trim(F.col("application_status")).cast(StringType()))
    .withColumn("workflow_state", F.trim(F.col("workflow_state")).cast(StringType()))
    .withColumn("rate_type", F.trim(F.col("rate_type")).cast(StringType()))
    .withColumn("collateral_type", F.trim(F.col("collateral_type")).cast(StringType()))
    .withColumn("pricing_basis", F.trim(F.col("pricing_basis")).cast(StringType()))

    # Dates
    .withColumn("application_date", F.col("application_date").cast(TimestampType()))
    .withColumn("decision_at", F.col("decision_at").cast(TimestampType()))
    .withColumn("booked_at", F.col("booked_at").cast(TimestampType()))
    .withColumn("disbursed_at", F.col("disbursed_at").cast(TimestampType()))

    # Amounts & Rates
    .withColumn("requested_amount", F.col("requested_amount").cast(DoubleType()))
    .withColumn("amount_granted", F.col("amount_granted").cast(DoubleType()))
    .withColumn("monthly_installment", F.col("monthly_installment").cast(DoubleType()))
    .withColumn("nominal_rate", F.col("nominal_rate").cast(DoubleType()))
    .withColumn("apr", F.col("apr").cast(DoubleType()))

    # Risk fields
    .withColumn("ifrs9_pd_12m", F.col("ifrs9_pd_12m").cast(DoubleType()))
    .withColumn("ifrs9_lgd", F.col("ifrs9_lgd").cast(DoubleType()))
    .withColumn("ifrs9_ecl_12m", F.col("ifrs9_ecl_12m").cast(DoubleType()))

    # Boolean
    .withColumn("affordability_pass", F.col("affordability_pass").cast(BooleanType()))
)

print("✅ Schema enforced")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Fixes + Derived Columns

# CELL ********************

silver = (
    silver
    # Fix negative discretionary income
    .withColumn("discretionary_income",
        F.greatest(F.col("discretionary_income"), F.lit(0.0))
    )

    # Clean loan status
    .withColumn("loan_status",
        F.when(F.col("workflow_state") == "Booked", "Disbursed")
         .when(F.col("workflow_state") == "Declined", "Rejected")
         .when(F.col("workflow_state") == "Withdrawn", "Withdrawn")
         .otherwise("Unknown")
    )
    .withColumn("is_booked", F.col("workflow_state") == "Booked")
    .withColumn("is_disbursed", F.col("disbursed_at").isNotNull())
    .withColumn("is_rejected", F.col("loan_status") == "Rejected")

    # Amount flags
    .withColumn("is_fully_granted",
        (F.col("amount_granted") >= F.col("requested_amount") * 0.99)
    )

    # Date derived
    .withColumn("application_year", F.year("application_date"))
    .withColumn("application_month", F.month("application_date"))
    .withColumn("days_to_decision",
        F.when(F.col("decision_at").isNotNull(),
            F.datediff(F.col("decision_at"), F.col("application_date")))
    )

    # LTV handling
    .withColumn("loan_to_value_ratio",
        F.when(F.col("collateral_type") != "none", F.col("loan_to_value_ratio"))
    )

    # Audit columns
    .withColumn("record_source", F.lit("bronze_loans"))
    .withColumn("silver_batch_id", F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp", F.current_timestamp())
)

print("✅ Derived columns & fixes applied")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver (Delta Merge)

# CELL ********************

def merge_silver(df, table_name, business_key):
    if not spark.catalog.tableExists(table_name):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"✅ Table {table_name} created")
        return df.count(), 0

    target = DeltaTable.forName(spark, table_name)
    update_set = {c: f"s.{c}" for c in df.columns if c != "created_timestamp"}
    update_set["silver_load_timestamp"] = "current_timestamp()"

    target.alias("t").merge(
        df.alias("s"), f"t.{business_key} = s.{business_key}"
    ).whenMatchedUpdate(set=update_set) \
     .whenNotMatchedInsertAll() \
     .execute()

    print(f"✅ Merged into {table_name}")
    return df.count(), 0  # adjust if you track updates

rows_processed, rows_updated = merge_silver(silver, TARGET_TABLE, "loan_id")
print(f"Rows processed: {rows_processed:,}")

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
    "pipeline_name": PIPELINE_NAME,
    "source_table": SOURCE_TABLE,
    "watermark_column": WATERMARK_COL,
    "watermark_value": new_watermark,
    "batch_id": SILVER_BATCH_ID,
    "rows_processed": rows_processed,
    "rows_inserted": rows_processed,
    "rows_updated": rows_updated,
    "status": "SUCCESS",
    "processed_timestamp": datetime.datetime.utcnow()
}]).write.format("delta").mode("append").saveAsTable("control.batch_watermark")

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
    "rows_processed": rows_processed,
    "rows_inserted": rows_processed,
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

# CELL ********************

df = spark.sql("SELECT * FROM lh_silver_banking_data.control.silver_audit_log LIMIT 1000")
display(df)

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
