# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "93bf9384-b2f4-4582-8e72-f990c6a5e82b",
# META       "default_lakehouse_name": "lh_gold_banking_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "93bf9384-b2f4-4582-8e72-f990c6a5e82b"
# META         },
# META         {
# META           "id": "a03cfff1-048d-457c-8848-da958470832d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Transform
# 
# **Notebook:** `300_001_build_dim_date_gold`  
# **Source:** `lh_silver_banking_data.transactions` (date range derivation only)  
# **Target:** `lh_gold_banking_data.dim_date`  
# **Layer:** Gold  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Derive date range from Silver `transactions` — `min` / `max` of `transaction_date` |
# | 2 | Generate a full date spine using `pd.date_range()` |
# | 3 | Convert to Spark DataFrame and compute all calendar attributes |
# | 4 | Flag South African public holidays (hardcoded lookup) |
# | 5 | Write `dim_date` to Gold Lakehouse — full overwrite (idempotent) |
# | 6 | Audit log update |
# 
# ---
# 
# ## Design Notes
# 
# - **No watermark** — `dim_date` is fully regenerated on every run. It is small (~3–5 K rows) and generation is near-instant.  
# - **No PII** — pure calendar data, no customer or account attributes.  
# - **`date_key`** is an integer in `YYYYMMDD` format — used as FK in `fact_transaction`.  
# - SA public holidays are hardcoded for the years covered by the dataset. Add future years as needed.  
# - Re-running this notebook is always safe — `overwrite` mode replaces the table entirely.


# MARKDOWN ********************

# ## Configuration & Imports

# CELL ********************

import datetime
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DateType, StringType, BooleanType
)

# Batch identity
GOLD_BATCH_ID   = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME   = "300_001_build_dim_date_gold"
SOURCE_TABLE    = "lh_silver_banking_data.dbo.transactions"
TARGET_TABLE    = "dim_date"
START_TIME      = datetime.datetime.utcnow()
WATERMARK_COL = "_ingest_timestamp"

print(f"Gold batch  : {GOLD_BATCH_ID}")
print(f"Pipeline    : {PIPELINE_NAME}")
print(f"Source      : {SOURCE_TABLE}")
print(f"Target      : {TARGET_TABLE}")


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
CREATE TABLE IF NOT EXISTS control.gold_audit_log (
    pipeline_name    STRING,
    batch_id         STRING,
    source_table     STRING,
    target_table     STRING,
    rows_read        BIGINT,
    rows_inserted    BIGINT,
    rows_expired     BIGINT,
    start_timestamp  TIMESTAMP,
    end_timestamp    TIMESTAMP,
    status           STRING
)
USING DELTA
""")

print("✅ Control tables ready")

print("✅ Control tables ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Derive Date Range from Silver Transactions
# 
# The date spine covers `min(transaction_date)` to `max(transaction_date)` in Silver.
# We add a 1-year buffer on both ends so the dim is never the limiting factor
# when future data arrives.

# CELL ********************

txn = spark.table(SOURCE_TABLE)

min_date_row = txn.agg(F.min("transaction_date")).collect()[0][0]
max_date_row = txn.agg(F.max("transaction_date")).collect()[0][0]

# Add buffer: 1 year before earliest, 1 year after latest
date_start = (pd.Timestamp(min_date_row) - pd.DateOffset(years=1)).date()
date_end   = (pd.Timestamp(max_date_row) + pd.DateOffset(years=1)).date()

print(f"Transaction date range : {min_date_row}  →  {max_date_row}")
print(f"Dim date spine         : {date_start}  →  {date_end}")
print(f"Total days to generate : {(pd.Timestamp(date_end) - pd.Timestamp(date_start)).days + 1:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## South African Public Holiday Lookup
# 
# Hardcoded for the years covered by the dataset.
# Add future years to `SA_PUBLIC_HOLIDAYS` as needed.
# Source: https://www.gov.za/about-sa/public-holidays

# CELL ********************

# South African public holidays (YYYY-MM-DD strings)
# Covers 2018–2027 — extend as needed
SA_PUBLIC_HOLIDAYS = set([
    # 2018
    "2018-01-01","2018-03-21","2018-03-30","2018-04-02",
    "2018-04-27","2018-05-01","2018-06-16","2018-08-09",
    "2018-09-24","2018-12-16","2018-12-25","2018-12-26",
    # 2019
    "2019-01-01","2019-03-21","2019-04-19","2019-04-22",
    "2019-04-27","2019-05-01","2019-06-17","2019-08-09",
    "2019-09-24","2019-12-16","2019-12-25","2019-12-26",
    # 2020
    "2020-01-01","2020-03-21","2020-04-10","2020-04-13",
    "2020-04-27","2020-05-01","2020-06-16","2020-08-10",
    "2020-09-24","2020-12-16","2020-12-25","2020-12-26",
    # 2021
    "2021-01-01","2021-03-21","2021-04-02","2021-04-05",
    "2021-04-27","2021-05-01","2021-05-03","2021-06-16",
    "2021-08-09","2021-09-24","2021-12-16","2021-12-25","2021-12-26","2021-12-27",
    # 2022
    "2022-01-01","2022-03-21","2022-04-15","2022-04-18",
    "2022-04-27","2022-05-02","2022-06-16","2022-06-17",
    "2022-08-09","2022-09-24","2022-12-16","2022-12-25","2022-12-26","2022-12-27",
    # 2023
    "2023-01-01","2023-01-02","2023-03-21","2023-04-07",
    "2023-04-10","2023-04-27","2023-05-01","2023-06-16",
    "2023-08-09","2023-09-24","2023-09-25","2023-12-16","2023-12-25","2023-12-26",
    # 2024
    "2024-01-01","2024-03-21","2024-03-29","2024-04-01",
    "2024-04-27","2024-05-01","2024-05-29","2024-06-17",
    "2024-08-09","2024-09-24","2024-12-16","2024-12-25","2024-12-26",
    # 2025
    "2025-01-01","2025-03-21","2025-04-18","2025-04-21",
    "2025-04-28","2025-05-01","2025-06-16","2025-08-09",
    "2025-09-24","2025-12-16","2025-12-25","2025-12-26",
    # 2026
    "2026-01-01","2026-03-21","2026-04-03","2026-04-06",
    "2026-04-27","2026-05-01","2026-06-16","2026-08-10",
    "2026-09-24","2026-12-16","2026-12-25","2026-12-26",
    # 2027
    "2027-01-01","2027-03-21","2027-03-26","2027-03-29",
    "2027-04-27","2027-05-01","2027-06-16","2027-08-09",
    "2027-09-24","2027-12-16","2027-12-25","2027-12-26",
])

print(f"✅ SA public holidays loaded : {len(SA_PUBLIC_HOLIDAYS)} dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate Date Spine
# 
# Build a pandas DataFrame covering every calendar date in the range,
# compute all attributes, then convert to Spark.

# CELL ********************

date_range = pd.date_range(start=date_start, end=date_end, freq="D")

rows = []
for d in date_range:
    ds        = d.strftime("%Y-%m-%d")
    date_key  = int(d.strftime("%Y%m%d"))
    quarter   = (d.month - 1) // 3 + 1

    # Month-end: last day of the month
    is_month_end = (d + pd.Timedelta(days=1)).month != d.month

    # Quarter-end: last day of Q1/Q2/Q3/Q4
    is_quarter_end = is_month_end and d.month in (3, 6, 9, 12)

    rows.append({
        "date_key"            : date_key,
        "full_date"           : d.date(),
        "year"                : d.year,
        "month"               : d.month,
        "day"                 : d.day,
        "quarter"             : quarter,
        "week_of_year"        : int(d.strftime("%W")),   # Monday-based week
        "day_of_week"         : d.dayofweek + 1,         # 1=Monday … 7=Sunday
        "day_name"            : d.strftime("%A"),
        "month_name"          : d.strftime("%B"),
        "is_weekend"          : d.dayofweek >= 5,
        "is_month_end"        : is_month_end,
        "is_quarter_end"      : is_quarter_end,
        "is_public_holiday_za": ds in SA_PUBLIC_HOLIDAYS,
    })

pdf = pd.DataFrame(rows)
print(f"✅ Date spine generated : {len(pdf):,} rows")
print(f"   Date range : {pdf['full_date'].min()}  →  {pdf['full_date'].max()}")
pdf.head(3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Convert to Spark & Validate Schema

# CELL ********************

schema = StructType([
    StructField("date_key",             IntegerType(), False),
    StructField("full_date",            DateType(),    False),
    StructField("year",                 IntegerType(), False),
    StructField("month",                IntegerType(), False),
    StructField("day",                  IntegerType(), False),
    StructField("quarter",              IntegerType(), False),
    StructField("week_of_year",         IntegerType(), False),
    StructField("day_of_week",          IntegerType(), False),
    StructField("day_name",             StringType(),  False),
    StructField("month_name",           StringType(),  False),
    StructField("is_weekend",           BooleanType(), False),
    StructField("is_month_end",         BooleanType(), False),
    StructField("is_quarter_end",       BooleanType(), False),
    StructField("is_public_holiday_za", BooleanType(), False),
])

dim_date = spark.createDataFrame(pdf, schema=schema)

# Add audit columns
dim_date = (
    dim_date
    .withColumn("gold_batch_id",        F.lit(GOLD_BATCH_ID))
    .withColumn("gold_load_timestamp",  F.current_timestamp())
)

print(f"✅ Spark DataFrame created : {dim_date.count():,} rows, {len(dim_date.columns)} columns")
dim_date.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation

# CELL ********************

print("=" * 55)
print("PRE-WRITE VALIDATION — dim_date")
print("=" * 55)

# 1. No duplicate date_key
dupe_keys = dim_date.groupBy("date_key").count().filter(F.col("count") > 1).count()
print(f"  Duplicate date_key          : {dupe_keys}  {'✅' if dupe_keys == 0 else '❌ FAIL'}")

# 2. No nulls on required columns
for col in ["date_key", "full_date", "year", "month", "day", "quarter"]:
    n = dim_date.filter(F.col(col).isNull()).count()
    print(f"  Nulls on {col:<20}: {n}  {'✅' if n == 0 else '❌ FAIL'}")

# 3. Public holiday spot checks
holidays_count = dim_date.filter(F.col("is_public_holiday_za") == True).count()
print(f"  Public holidays flagged     : {holidays_count}")

# 4. Weekend distribution sanity (should be ~2/7 of all days)
weekend_pct = dim_date.filter(F.col("is_weekend") == True).count() / dim_date.count() * 100
print(f"  Weekend % (expect ~28.6%)   : {weekend_pct:.1f}%  {'✅' if 27 < weekend_pct < 30 else '⚠️  Check'}")

# 5. Quarter distribution
print("\nQuarter distribution:")
dim_date.groupBy("quarter").count().orderBy("quarter").show()

print("Month-end flag count (expect ~12 per year):")
dim_date.filter(F.col("is_month_end") == True).groupBy("year").count().orderBy("year").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Gold Lakehouse
# 
# Full overwrite — `dim_date` is idempotent and regenerated from scratch each run.
# No merge required.

# CELL ********************

(
    dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

rows_written = dim_date.count()
print(f"✅ Written to {TARGET_TABLE} : {rows_written:,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Audit Log Update

# CELL ********************

END_TIME = datetime.datetime.utcnow()

spark.createDataFrame([{
    "pipeline_name"   : PIPELINE_NAME,
    "batch_id"        : GOLD_BATCH_ID,
    "source_table"    : SOURCE_TABLE,
    "target_table"    : TARGET_TABLE,
    "rows_read"       : rows_written,
    "rows_inserted"   : rows_written,
    "rows_expired"    : 0,
    "start_timestamp" : START_TIME,
    "end_timestamp"   : END_TIME,
    "status"          : "SUCCESS",
}]).write.format("delta").mode("append").saveAsTable("control.gold_audit_log")

print(f"✅ Audit log updated")
print(f"   Duration : {(END_TIME - START_TIME).total_seconds():.1f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary

# CELL ********************

from pyspark.sql import functions as F

dim = spark.table(TARGET_TABLE)

summary_df = spark.createDataFrame(
    [(
        GOLD_BATCH_ID,
        TARGET_TABLE,
        dim.count(),
        dim.agg(F.min("full_date")).first()[0],
        dim.agg(F.max("full_date")).first()[0],
        round((END_TIME - START_TIME).total_seconds(), 1)
    )],
    [
        "batch_id",
        "target_table",
        "rows_written",
        "min_date",
        "max_date",
        "duration_seconds"
    ]
)

print("=" * 65)
print("GOLD TRANSFORM SUMMARY — dim_date")
print("=" * 65)

display(summary_df)

print("Sample rows")
display(
    dim.orderBy("date_key").limit(5)
)

print("Public holidays sample")
display(
    dim.filter(F.col("is_public_holiday_za"))
       .orderBy("date_key")
       .limit(10)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
