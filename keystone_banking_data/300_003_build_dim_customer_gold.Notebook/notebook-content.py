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
# **Notebook:** `300_003_build_dim_customer_gold`  
# **Sources:**
# - `lh_silver_banking_data.customers_individual`
# - `lh_silver_banking_data.customers_non_individual`  
# 
# **Target:** `lh_gold_banking_data.dim_customer`  
# **Layer:** Gold  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Read Silver `customers_individual` and `customers_non_individual` |
# | 2 | Project Gold-relevant columns only — drop all raw PII, DQ internals, Bronze metadata |
# | 3 | Union both tables with `customer_type` as discriminator; business-only columns are `null` for individuals and vice versa |
# | 4 | Compute `customer_sk` surrogate key (carried from Silver — already `xxhash64(customer_id)`) |
# | 5 | Data quality checks |
# | 6 | Write to Gold — Type 1 merge on `customer_sk` |
# | 7 | Audit log update |
# 
# ---
# 
# ## Design Notes
# 
# - **SCD Type 1** — overwrite-in-place on match. Customer attributes (segment, risk tier, income band) reflect the latest Silver snapshot. No history is kept at Gold; Silver is the system of record for point-in-time customer state.  
# - **No PII** — `email`, `phone_number`, `id_number`, `tax_id_number`, `residential_address`, `commercial_address`, `next_of_kin` are all excluded from Gold. `full_name` is retained for display purposes only.  
# - **`customer_sk`** is inherited from Silver (`xxhash64(customer_id)`) — no recomputation needed. It is the FK used in `fact_transaction`.  
# - **Unified table** — Individual and Business rows coexist in one `dim_customer` table, discriminated by `customer_type`. Business-only columns (`business_segment`, `annual_turnover`, `company_size`, `vat_registered`) are `null` for Individual rows. Individual-only columns (`age_band`, `gender`, `marital_status`, `income_band`) are `null` for Business rows.


# MARKDOWN ********************

# ## Configuration & Imports

# CELL ********************

import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, BooleanType, IntegerType, DoubleType, LongType
)
from delta.tables import DeltaTable

GOLD_BATCH_ID   = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME   = "300_003_build_dim_customer_gold"
SOURCE_IND      = "lh_silver_banking_data.dbo.customers_individual"
SOURCE_BUS      = "lh_silver_banking_data.dbo.customers_non_individual"
TARGET_TABLE    = "dim_customer"
START_TIME      = datetime.datetime.utcnow()

print(f"Gold batch  : {GOLD_BATCH_ID}")
print(f"Pipeline    : {PIPELINE_NAME}")
print(f"Sources     : {SOURCE_IND}")
print(f"              {SOURCE_BUS}")
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

# ## Read Silver Sources
# 
# Project Gold-relevant columns only from each Silver table.
# PII columns (`email`, `phone_number`, `id_number`, `tax_id_number`,
# `residential_address`, `commercial_address`, `next_of_kin`) are excluded here —
# they never enter the Gold layer.

# CELL ********************

# Individual customers
silver_ind = spark.table(SOURCE_IND)

ind = (
    silver_ind
    .select(
        # Keys
        F.col("customer_id"),
        F.col("customer_sk"),
        F.col("customer_type"),   # Individual — passed through from Silver as-is

        # Display
        F.col("full_name"),

        # Demographics — Individual only
        F.col("age_band"),
        F.col("gender"),
        F.col("marital_status"),
        F.col("education_level"),
        F.col("occupation"),

        # Financial — Individual only
        F.col("income_band"),
        F.col("annual_income"),

        # Business — null for Individual
        F.lit(None).cast(StringType()).alias("business_segment"),
        F.lit(None).cast(DoubleType()).alias("annual_turnover"),
        F.lit(None).cast(StringType()).alias("company_size"),
        F.lit(None).cast(BooleanType()).alias("vat_registered"),

        # Segmentation (shared)
        F.col("customer_segment"),
        F.col("tenure_band"),
        F.col("risk_segment"),
        F.col("is_digital_customer"),
        F.col("completeness_score"),
        F.col("segmentation_ready"),

        # Risk & compliance (shared)
        F.col("kyc_risk_tier"),
        F.col("is_high_risk"),
        F.col("is_pep"),
        F.col("is_foreign_national"),
        F.col("nationality"),

        # Citizenship — Individual only
        F.col("is_dual_citizen"),
        F.col("primary_citizenship"),

        # Branch (shared)
        F.col("branch_id"),
        F.col("branch_name"),
        F.col("branch_city"),
        F.col("branch_province"),

        # Channel (shared)
        F.col("capture_channel"),

        # Silver audit passthrough
        F.col("silver_load_timestamp"),
    )
)

print(f"Individual rows  : {ind.count():,}")

# Business / rganization customers
silver_bus = spark.table(SOURCE_BUS)

bus = (
    silver_bus
    .select(
        # Keys
        F.col("customer_id"),
        F.col("customer_sk"),
        F.col("customer_type"),

        # Display
        F.col("full_name"),

        # Demographics — null for Business
        F.lit(None).cast(StringType()).alias("age_band"),
        F.lit(None).cast(StringType()).alias("gender"),
        F.lit(None).cast(StringType()).alias("marital_status"),
        F.lit(None).cast(StringType()).alias("education_level"),
        F.lit(None).cast(StringType()).alias("occupation"),

        # Financial — null for Business (use annual_turnover instead)
        F.lit(None).cast(StringType()).alias("income_band"),
        F.lit(None).cast(DoubleType()).alias("annual_income"),

        # Business — Business only
        F.col("business_segment"),
        F.col("annual_turnover"),
        F.col("company_size"),
        F.col("vat_registered"),

        # Segmentation (shared)
        F.lit(None).cast(StringType()).alias("customer_segment"),
        F.col("tenure_band"),
        F.col("risk_segment"),
        F.col("is_digital_customer"),
        F.col("completeness_score"),
        F.col("segmentation_ready"),

        # Risk & compliance (shared)
        F.col("kyc_risk_tier"),
        F.col("is_high_risk"),
        F.col("is_pep"),
        F.lit(None).cast(BooleanType()).alias("is_foreign_national"),
        F.col("nationality"),

        # Citizenship — null for Business
        F.lit(None).cast(BooleanType()).alias("is_dual_citizen"),
        F.lit(None).cast(StringType()).alias("primary_citizenship"),

        # Branch (shared)
        F.col("branch_id"),
        F.col("branch_name"),
        F.col("branch_city"),
        F.col("branch_province"),

        # Channel (shared)
        F.col("capture_channel"),

        # Silver audit passthrough
        F.col("silver_load_timestamp"),
    )
)

print(f"Business rows    : {bus.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Union & Add Gold Audit Columns

# CELL ********************

dim_customer = (
    ind.unionByName(bus)
    .withColumn("gold_batch_id",       F.lit(GOLD_BATCH_ID))
    .withColumn("gold_load_timestamp", F.current_timestamp())
)

total = dim_customer.count()
print(f"✅ Unified dim_customer : {total:,} rows")
print(f"   Individual  : {dim_customer.filter(F.col('customer_type') == 'Individual').count():,}")
print(f"   Company     : {dim_customer.filter(F.col('customer_type') == 'Company').count():,}")
print(f"   Organization: {dim_customer.filter(F.col('customer_type') == 'Organization').count():,}")
dim_customer.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality Checks

# CELL ********************

print("=" * 60)
print("DQ CHECK — dim_customer")
print("=" * 60)

# 1. Duplicate customer_sk
dupe_sk = dim_customer.groupBy("customer_sk").count().filter(F.col("count") > 1).count()
print(f"  {'✅' if dupe_sk == 0 else '❌ FAIL'}  Duplicate customer_sk    : {dupe_sk}")

# 2. Duplicate customer_id
dupe_id = dim_customer.groupBy("customer_id").count().filter(F.col("count") > 1).count()
print(f"  {'✅' if dupe_id == 0 else '❌ FAIL'}  Duplicate customer_id    : {dupe_id}")

# 3. Required nulls
for col in ["customer_id", "customer_sk", "customer_type", "kyc_risk_tier"]:
    n = dim_customer.filter(F.col(col).isNull()).count()
    print(f"  {'✅' if n == 0 else '❌ FAIL'}  Nulls on {col:<25}: {n:,}")

# 4. PII guard — none of these columns should exist
PII_COLS = ["email", "phone_number", "id_number", "tax_id_number",
            "residential_address", "commercial_address", "next_of_kin"]
pii_present = [c for c in PII_COLS if c in dim_customer.columns]
if pii_present:
    print(f"  ❌ FAIL  PII columns present in Gold: {pii_present}")
else:
    print(f"  ✅  No PII columns in dim_customer")

# 5. Customer type distribution — must show Individual, Company, Organization
print("\nCustomer type distribution (expect: Individual, Company, Organization):")
dim_customer.groupBy("customer_type").count().orderBy("count", ascending=False).show()

valid_types = {"Individual", "Company", "Organization"}
unknown_types = dim_customer.filter(~F.col("customer_type").isin(list(valid_types))).count()
print(f"  {'✅' if unknown_types == 0 else '❌ FAIL'}  Unknown customer_type values : {unknown_types}")

# 6. Risk tier distribution
print("KYC risk tier distribution:")
dim_customer.groupBy("kyc_risk_tier").count().orderBy("count", ascending=False).show()

# 7. Segmentation readiness
ready = dim_customer.filter(F.col("segmentation_ready") == True).count()
print(f"Segmentation ready : {ready:,} / {total:,} ({ready/total*100:.1f}%)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## `merge_gold()` — Type 1 Upsert
# 
# Matches on `customer_sk`. On match: overwrite all columns (Type 1 — no history).
# On no match: insert new row.

# CELL ********************

def merge_gold(df, table_name, business_key):
    """Type 1 Gold merge — overwrite-in-place on match, insert on no match."""

    if not spark.catalog.tableExists(table_name):
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        inserts = df.count()
        updates = 0
        print(f"✅ Created {table_name} : {inserts:,} rows (first load)")
        return inserts, updates

    existing_keys = spark.table(table_name).select(business_key)

    inserts = df.join(existing_keys, business_key, "left_anti").count()
    updates = df.count() - inserts

    target = DeltaTable.forName(spark, table_name)

    update_set = {c: f"s.{c}" for c in df.columns}

    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{business_key} = s.{business_key}")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged {table_name}")
    print(f"   Inserts : {inserts:,}")
    print(f"   Updates : {updates:,}")
    return inserts, updates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Gold Lakehouse

# CELL ********************

rows_inserted, rows_updated = merge_gold(
    dim_customer,
    TARGET_TABLE,
    "customer_sk"
)

rows_written = rows_inserted + rows_updated
print(f"\n✅ dim_customer written : {rows_written:,} rows total")

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
    "source_table"    : f"{SOURCE_IND} + {SOURCE_BUS}",
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
print("  GOLD TRANSFORM SUMMARY — dim_customer")
print("=" * 65)
print(f"""
  Batch ID       : {GOLD_BATCH_ID}
  Sources        : {SOURCE_IND}
                   {SOURCE_BUS}
  Target         : {TARGET_TABLE}
  Rows written   : {rows_written:,}
    Inserts      : {rows_inserted:,}
    Updates      : {rows_updated:,}
  Duration       : {(END_TIME - START_TIME).total_seconds():.1f}s
""")

print("── Customer type split (Individual / Company / Organization) ──")
dim.groupBy("customer_type").count().orderBy("count", ascending=False).show()

print("── KYC risk tier ──")
dim.groupBy("kyc_risk_tier").count().orderBy("count", ascending=False).show()

print("── Branch province (Individual) ──")
dim.filter(F.col("customer_type") == "Individual") \
   .groupBy("branch_province").count().orderBy("count", ascending=False).show()

print("── Business segment ──")
dim.filter(F.col("customer_type") != "Individual") \
   .groupBy("business_segment").count().orderBy("count", ascending=False).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete customers row from both control tables

spark.sql("""
    DELETE FROM lh_gold_banking_data.control.batch_watermark
    WHERE pipeline_name = '300_003_build_dim_customer_gold'
""")

spark.sql("""
    DELETE FROM lh_gold_banking_data.control.gold_audit_log
    WHERE pipeline_name = '300_003_build_dim_customer_gold'
""")

print("✅ Deleted customers rows from both control tables")

# Verify
spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_gold_banking_data.control.batch_watermark
    WHERE pipeline_name = '300_003_build_dim_customer_gold'
""").show(truncate=False)

spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_gold_banking_data.control.gold_audit_log
    WHERE pipeline_name = '300_003_build_dim_customer_gold'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
