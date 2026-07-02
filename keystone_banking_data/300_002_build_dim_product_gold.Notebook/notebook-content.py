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
# **Notebook:** `300_002_build_dim_product_gold`  
# **Source:** `lh_silver_banking_data.account_product_enrollments`  
# **Target:** `lh_gold_banking_data.dim_product`  
# **Layer:** Gold  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Read distinct `product_code` values from Silver `account_product_enrollments` |
# | 2 | Enrich with hardcoded lookup: `product_name`, `product_category`, `is_credit_product` |
# | 3 | Derive `is_active` — True if any active enrollment exists for the product |
# | 4 | Flag unknown codes not in the lookup map (DQ guard) |
# | 5 | Write `dim_product` to Gold — full overwrite (small static reference table) |
# | 6 | Audit log update |
# 
# ---
# 
# ## Design Notes
# 
# - **No surrogate key** — `product_code` is the natural PK; it is short, stable, and used directly as FK in `fact_transaction`.  
# - **No watermark** — full overwrite on every run. The table is tiny (~10–20 rows).  
# - **No PII** — purely a product reference table.  
# - The enrichment lookup map is the single source of truth for `product_name` and `product_category`. If a new `product_code` appears in Silver that is not in the map, it is flagged in the DQ summary and written with `product_name = product_code` as a safe fallback.


# MARKDOWN ********************

# ## Configuration & Imports

# CELL ********************

import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType
)

GOLD_BATCH_ID  = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME  = "300_002_build_dim_product_gold"
SOURCE_TABLE   = "lh_silver_banking_data.dbo.account_product_enrollments"
TARGET_TABLE   = "dim_product"
START_TIME     = datetime.datetime.utcnow()

print(f"Gold batch : {GOLD_BATCH_ID}")
print(f"Pipeline   : {PIPELINE_NAME}")
print(f"Source     : {SOURCE_TABLE}")
print(f"Target     : {TARGET_TABLE}")

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Product Enrichment Lookup Map
# 
# Hardcoded mapping of `product_code` → display attributes.  
# Known codes derived from Silver `200_014` profiling. Update this map as new product codes appear in Silver.

# CELL ********************

# product_code → (product_name, product_category, is_credit_product)
PRODUCT_LOOKUP = {
    # Credit
    "credit_card"           : ("Credit Card",            "Credit",    True),
    "overdraft_facility"    : ("Overdraft Facility",     "Credit",    True),
    "business_credit_line"  : ("Business Credit Line",   "Credit",    True),

    # Deposit / transactional
    "savings_account"       : ("Savings Account",        "Deposit",   False),
    "current_account"       : ("Current Account",        "Deposit",   False),
    "cheque_account"        : ("Cheque Account",         "Deposit",   False),
    "fixed_deposit"         : ("Fixed Deposit",          "Deposit",   False),
    "money_market"          : ("Money Market Account",   "Deposit",   False),
    "notice_account"        : ("Notice Account",         "Deposit",   False),

    # Lending 
    "personal_loan"         : ("Personal Loan",          "Lending",   True),
    "home_loan"             : ("Home Loan",              "Lending",   True),
    "vehicle_finance"       : ("Vehicle Finance",        "Lending",   True),
    "student_loan"          : ("Student Loan",           "Lending",   True),

    # Insurance / value-add 
    "funeral_cover"         : ("Funeral Cover",          "Insurance", False),
    "life_insurance"        : ("Life Insurance",         "Insurance", False),
    "travel_insurance"      : ("Travel Insurance",       "Insurance", False),
    "short_term_insurance"  : ("Short-term Insurance",   "Insurance", False),

    # Digital / bundle 
    "online_banking"        : ("Online Banking",         "Digital",   False),
    "mobile_banking"        : ("Mobile Banking",         "Digital",   False),
    "rewards_programme"     : ("Rewards Programme",      "Bundle",    False),
}

print(f"✅ Product lookup map loaded : {len(PRODUCT_LOOKUP)} entries")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Silver & Discover All Product Codes
# 
# Pull distinct `product_code` values with enrollment counts and active status.

# CELL ********************

enrollments = spark.table(SOURCE_TABLE)

product_stats = (
    enrollments
    .groupBy("product_code")
    .agg(
        F.count("*").alias("total_enrollments"),
        F.sum(F.col("is_active_enrollment").cast("int")).alias("active_enrollments")
    )
    .withColumn("is_active", F.col("active_enrollments") > 0)
)

total_codes = product_stats.count()
print(f"Distinct product codes in Silver : {total_codes}")
product_stats.orderBy("product_code").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enrich with Lookup Map
# 
# Left join Silver product codes against the lookup map.
# Unknown codes receive a safe fallback: `product_category = 'Unknown'`


# CELL ********************

lookup_rows = [
    (code, name, category, is_credit)
    for code, (name, category, is_credit) in PRODUCT_LOOKUP.items()
]

lookup_schema = StructType([
    StructField("product_code",      StringType(),  False),
    StructField("product_name",      StringType(),  True),
    StructField("product_category",  StringType(),  True),
    StructField("is_credit_product", BooleanType(), True),
])

lookup_df = spark.createDataFrame(lookup_rows, schema=lookup_schema)

dim_product = (
    product_stats
    .join(lookup_df, on="product_code", how="left")
    .withColumn("product_name",
        F.coalesce(F.col("product_name"), F.col("product_code"))
    )
    .withColumn("product_category",
        F.coalesce(F.col("product_category"), F.lit("Unknown"))
    )
    .withColumn("is_credit_product",
        F.coalesce(F.col("is_credit_product"), F.lit(False))
    )
    .select(
        "product_code",
        "product_name",
        "product_category",
        "is_credit_product",
        "is_active",
        "total_enrollments",
        "active_enrollments",
    )
    .withColumn("gold_batch_id",       F.lit(GOLD_BATCH_ID))
    .withColumn("gold_load_timestamp", F.current_timestamp())
)

print(f"✅ Enrichment applied : {dim_product.count()} product rows")
dim_product.orderBy("product_category", "product_code").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Checks

# CELL ********************

print("=" * 55)
print("DQ CHECK — dim_product")
print("=" * 55)

unknown = dim_product.filter(F.col("product_category") == "Unknown")
unknown_count = unknown.count()

if unknown_count == 0:
    print("  ✅  All product codes resolved — no unknowns")
else:
    print(f"  ⚠️   {unknown_count} unknown product code(s) — add to PRODUCT_LOOKUP:")
    unknown.select("product_code", "total_enrollments").show(truncate=False)

for col in ["product_code", "product_name", "product_category", "is_credit_product"]:
    n = dim_product.filter(F.col(col).isNull()).count()
    print(f"  {'✅' if n == 0 else '❌ FAIL'}  Nulls on {col}: {n}")

print("\nCategory distribution:")
dim_product.groupBy("product_category").count().orderBy("product_category").show()

print(f"Credit products : {dim_product.filter(F.col('is_credit_product') == True).count()}")
print(f"Active products : {dim_product.filter(F.col('is_active') == True).count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Gold Lakehouse

# CELL ********************

(
    dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

rows_written = dim_product.count()
print(f"✅ Written to {TARGET_TABLE} : {rows_written} rows")

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

dim = spark.table(TARGET_TABLE)

print("=" * 65)
print("  GOLD TRANSFORM SUMMARY — dim_product")
print("=" * 65)
print(f"""
  Batch ID       : {GOLD_BATCH_ID}
  Source         : {SOURCE_TABLE}
  Target         : {TARGET_TABLE}
  Rows written   : {dim.count()}
  Duration       : {(END_TIME - START_TIME).total_seconds():.1f}s
""")

dim.orderBy("product_category", "product_code").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete product row from both control tables

spark.sql("""
    DELETE FROM lh_gold_banking_data.control.batch_watermark
    WHERE pipeline_name = '300_002_build_dim_product_gold'
""")

spark.sql("""
    DELETE FROM lh_gold_banking_data.control.gold_audit_log
    WHERE pipeline_name = '300_002_build_dim_product_gold'
""")

print("✅ Deleted customers rows from both control tables")

# Verify
spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_gold_banking_data.control.batch_watermark
    WHERE pipeline_name = '300_002_build_dim_product_gold'
""").show(truncate=False)

spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_gold_banking_data.control.gold_audit_log
    WHERE pipeline_name = '300_002_build_dim_product_gold'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
