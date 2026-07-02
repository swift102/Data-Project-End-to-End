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
# **Notebook:** `200_002_transform_accounts_silver`  
# **Source:** `lh_bronze_banking_data_modern_data.dbo.bronze_accounts` (109,849 rows)  
# **Target:** `lh_silver_banking_data.accounts`  
# **Layer:** Silver  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Load Bronze + CDC dedup — keep latest record per `account_id` |
# | 2 | Enforce explicit schema — fix `closure_date` (integer → date), `monthly_charges` (long → double) |
# | 3 | Null classification — required vs conditionally null vs empty |
# | 4 | PII masking — `account_number`, `card_number`, `iban` |
# | 5 | Derived columns — `account_age_days`, `is_active`, `is_inactive`, `tier_label`, `has_overdraft`, `has_credit_card`, `is_foreign_currency` |
# | 6 | Flag embedded JSON columns — parse or defer to child tables |
# | 7 | Write `dim_accounts` to Silver + validation summary |
# 
# ---
# 
# ## Key Findings from Bronze Profiling
# 
# | Finding | Detail |
# |---|---|
# | CDC source | `cdc_op_hint`: 89,780 `I` (insert) + 20,061 `U` (update) — dedup on `record_last_updated_at` |
# | `closure_date` | All 109,841 values null — no closed accounts in dataset, column dropped |
# | `monthly_charges` | Typed as `long` in Bronze — cast to `double` (monetary field) |
# | Account distribution | 45,316 customers × 1 account, 32,237 × 2, 17 × 3 |
# | Status | 98,360 active (89.5%), 7,788 restricted, 2,046 suspended, 1,647 frozen |
# | Tier/type mapping | `basic`=easy/joint/savings · `standard`=cheque/current/business/aspire · `premium`=gold/platinum/premium |
# | Foreign accounts | `swift_code`/`iban` only on EUR/USD accounts (~5%) |
# | Embedded JSON | 4 JSON columns defer to child Bronze tables (do not parse here) |


# MARKDOWN ********************

# ## 1. Configuration & Imports

# CELL ********************

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, DateType, DoubleType,
    BooleanType, IntegerType, TimestampType
)

# Capture batch start time for accurate audit log timing
START_TIME = datetime.datetime.utcnow()


# Mask salt value
config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)
MASK_SALT = config["MASK_SALT"]

# Batch identity 
SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_accounts"
TARGET_TABLE    = "accounts"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Source       : {SOURCE_TABLE}")
print(f"Target       : {TARGET_TABLE}")


# Watermark Metadata

PIPELINE_NAME = "200_002_transform_accounts_silver"
WATERMARK_COL = "_ingest_timestamp"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Pipeline     : {PIPELINE_NAME}")
print(f"Source       : {SOURCE_TABLE}")

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

# ## 2. Load Bronze & CDC Dedup
# 
# `bronze_accounts` is a CDC feed — the source system emits both inserts (`I`) and updates (`U`).
# Silver keeps the **latest record per `account_id`** based on `record_last_updated_at`.
# Unlike `bronze_customers` where the same customer appeared in multiple monthly files,
# here `account_id` is already fully unique — the 20,061 `U` records are updates to
# existing accounts, not duplicates from monthly snapshots.

# CELL ********************

bronze = spark.table(SOURCE_TABLE)

if last_watermark is not None:
    bronze = bronze.filter(F.col("_ingest_timestamp") > F.lit(last_watermark))
    print(f"Incremental — records after {last_watermark}")
else:
    print("First run — full load")

# Cache the count — referenced again in watermark update
bronze_count = bronze.count()
print(f"Bronze rows loaded : {bronze_count:,}")
print("CDC breakdown:")
bronze.groupBy("cdc_op_hint").count().show()

# Keep latest record per account_id based on record_last_updated_at
w = Window.partitionBy("account_id").orderBy(
    F.col("record_last_updated_at").desc_nulls_last()
)

deduped = (
    bronze
    .withColumn("_row_rank", F.row_number().over(w))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank", "cdc_op_hint")  # cdc_op_hint not needed in Silver
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

# ## 3. Enforce Explicit Schema
# 
# Fixes three Bronze type issues:
# - `closure_date` typed as `integer` — dropped (all nulls, no closed accounts in dataset)
# - `monthly_charges` typed as `long` — cast to `double` (monetary)
# - All string fields trimmed and normalised

# CELL ********************

typed = (
    deduped

    # Identity
    .withColumn("account_id",             F.col("account_id").cast(StringType()))
    .withColumn("account_number",         F.trim(F.col("account_number")).cast(StringType()))
    .withColumn("customer_id",            F.col("customer_id").cast(StringType()))

    # Product classification
    .withColumn("bank_product_name",      F.trim(F.col("bank_product_name")).cast(StringType()))
    .withColumn("account_type",           F.trim(F.lower(F.col("account_type"))).cast(StringType()))
    .withColumn("account_tier",           F.trim(F.lower(F.col("account_tier"))).cast(StringType()))
    .withColumn("is_primary_account",     F.col("is_primary_account").cast(BooleanType()))

    # Dates
    .withColumn("opening_date",           F.col("opening_date").cast(DateType()))
    .withColumn("approval_date",          F.col("approval_date").cast(DateType()))
    .withColumn("status_change_date",     F.col("status_change_date").cast(DateType()))
    # closure_date: currently all-null (no closed accounts) but retained as nullable DateType.
    # Dropping it here would silently lose data once closed accounts appear in the source.
    .withColumn("closure_date",           F.col("closure_date").cast(DateType()))

    # Status & compliance
    .withColumn("account_status",         F.trim(F.lower(F.col("account_status"))).cast(StringType()))
    .withColumn("status_reason",          F.trim(F.col("status_reason")).cast(StringType()))
    .withColumn("branch_code",            F.trim(F.col("branch_code")).cast(StringType()))
    .withColumn("kyc_verified",           F.col("kyc_verified").cast(BooleanType()))
    .withColumn("fica_verified",          F.col("fica_verified").cast(BooleanType()))

    # Financial terms
    .withColumn("expected_amount",        F.col("expected_amount").cast(DoubleType()))
    .withColumn("interest_rate",          F.col("interest_rate").cast(DoubleType()))
    .withColumn("monthly_charges",        F.col("monthly_charges").cast(DoubleType()))  # was long
    .withColumn("transactions_rate",      F.col("transactions_rate").cast(DoubleType()))
    .withColumn("negative_balance_rate",  F.col("negative_balance_rate").cast(DoubleType()))
    .withColumn("overdraft_limit",        F.col("overdraft_limit").cast(DoubleType()))
    .withColumn("credit_card_limit",      F.col("credit_card_limit").cast(DoubleType()))

    # Card
    .withColumn("card_number",            F.col("card_number").cast(StringType()))
    .withColumn("card_type",              F.trim(F.lower(F.col("card_type"))).cast(StringType()))
    .withColumn("card_issue_date",        F.col("card_issue_date").cast(DateType()))
    .withColumn("card_expiry_date",       F.col("card_expiry_date").cast(DateType()))

    # Digital & channel
    .withColumn("online_banking_enabled",         F.col("online_banking_enabled").cast(BooleanType()))
    .withColumn("online_banking_activation_date", F.col("online_banking_activation_date").cast(DateType()))
    .withColumn("opening_channel",                F.trim(F.col("opening_channel")).cast(StringType()))
    .withColumn("statement_frequency",            F.trim(F.col("statement_frequency")).cast(StringType()))

    # Cross-border & currency
    .withColumn("currency",              F.trim(F.upper(F.col("currency"))).cast(StringType()))
    .withColumn("swift_code",            F.trim(F.col("swift_code")).cast(StringType()))
    .withColumn("iban",                  F.trim(F.col("iban")).cast(StringType()))
    .withColumn("cross_border_enabled",  F.col("cross_border_enabled").cast(BooleanType()))

    # Onboarding documents
    .withColumn("proof_of_income_provided",       F.col("proof_of_income_provided").cast(BooleanType()))
    .withColumn("proof_of_address_provided",      F.col("proof_of_address_provided").cast(BooleanType()))
    .withColumn("bank_statements_provided",       F.col("bank_statements_provided").cast(BooleanType()))
    .withColumn("employer_letter_provided",       F.col("employer_letter_provided").cast(BooleanType()))
    .withColumn("business_registration_provided", F.col("business_registration_provided").cast(BooleanType()))
    .withColumn("tax_certificate_provided",       F.col("tax_certificate_provided").cast(BooleanType()))
    .withColumn("minimum_deposit_met",            F.col("minimum_deposit_met").cast(BooleanType()))

    # Other
    .withColumn("linked_joint_accounts",  F.trim(F.col("linked_joint_accounts")).cast(StringType()))
    .withColumn("bundled_products",       F.trim(F.col("bundled_products")).cast(StringType()))
    .withColumn("beneficiaries",          F.trim(F.col("beneficiaries")).cast(StringType()))
    .withColumn("record_last_updated_at", F.col("record_last_updated_at").cast(TimestampType()))

    # Drop embedded JSON columns — deferred to child Bronze tables
    .drop("limits_history_json", "status_events_json",
          "product_enrollments_json", "signatories_json")
)

print("✅ Schema enforced")
print(f"Columns after typing : {len(typed.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Null Classification & Validation

# CELL ********************

REQUIRED = ["account_id", "account_number", "customer_id",
            "account_type", "account_tier", "account_status",
            "opening_date", "currency"]

print("=" * 55)
print("REQUIRED FIELD NULL CHECK")
print("=" * 55)
all_pass = True
for col in REQUIRED:
    n = typed.filter(F.col(col).isNull()).count()
    status = "✅" if n == 0 else "❌ FAIL"
    if n > 0: all_pass = False
    print(f"  {status}  {col}: {n:,} nulls")

print()
print("EXPECTED NULLS (by design)")
expected_nulls = {
    "status_change_date" : "null for active accounts",
    "status_reason"      : "null for active accounts",
    "swift_code"         : "null for ZAR accounts only",
    "iban"               : "null for ZAR accounts only",
    "linked_joint_accounts": "null for non-joint accounts",
    "overdraft_limit"    : "null for accounts without overdraft",
    "credit_card_limit"  : "null for accounts without credit card",
    "online_banking_activation_date": "null if online banking not enabled",
}
for col, reason in expected_nulls.items():
    if col in typed.columns:
        n = typed.filter(F.col(col).isNull()).count()
        print(f"  ✅  {col}: {n:,} nulls — {reason}")

print(f"\nAll required fields pass: {all_pass}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. PII Masking
# 
# Applied after schema enforcement and null classification, before derived columns.
# No derived column in this notebook reads `account_number`, `card_number`, or `iban`,
# so there is no ordering conflict — masking runs cleanly here.
# 
# **Techniques:**
# - **Partial mask** for `account_number` — last 4 digits visible, rest replaced with `****`
# - **Deterministic hash** for `card_number` — card network already captured in `card_type`/`card_category`
# - **Deterministic hash** for `iban` — not a join key, no structure worth preserving


# MARKDOWN ********************

# ## Masking Strategy
# 
# | Column | Technique | Reason |
# |---|---|---|
# | `account_number` | Partial mask — show last 4 digits | Not a join key; last-4 pattern is standard in banking UX and audit logs |
# | `card_number` | Deterministic hash (SHA-256 + salt) | Card network already captured in `card_type` and `card_category` |
# | `iban` | Deterministic hash (SHA-256 + salt) | Not a join key; no structure worth preserving |

# CELL ********************

masked = (
    typed

    # Partial mask: account_number 
    
    # Banks conventionally show only the last 4 digits of an account number
    #(e.g. "****4567"). This is enough for audit readability and customer
    # identification without exposing the full number.
    # We use length() to dynamically handle account numbers of varying lengths
    # rather than hardcoding a fixed substring position.
    # Why not hash? account_number isn't used as a join key in this pipeline,
    # and the last-4 pattern is standard practice in banking systems.
    
    .withColumn("account_number",
        F.when(
            F.col("account_number").isNotNull(),
            F.concat(
                F.lit("****"),
                F.substring(F.col("account_number"), -4, 4)
            )
        ).otherwise(F.lit(None).cast(StringType()))                         # account_number should be ****NNNN
    )

    # Deterministic hash: card_number 
     
    # Full hash — the card network (Visa/Mastercard/Amex) is already captured
    # in the `card_type` column and will be derived into `card_category`.
    # There is no analytical value in keeping any part of the raw card number.
    # SHA-256 + salt prevents rainbow table reversal.
    .withColumn("card_number",
        F.when(
            F.col("card_number").isNotNull(),
            F.sha2(F.concat(F.col("card_number"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))                        # card_number should be a 64-char hex string
    )

    # Deterministic hash: iban
    
    #IBAN is only present on EUR/USD accounts.
    #Not used as a join key anywhere in the pipeline.
    #Full hash removes all country and bank routing information.
    .withColumn("iban",
        F.when(
            F.col("iban").isNotNull(),
            F.sha2(F.concat(F.col("iban"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))                 # iban should be a 64-char hex string or null
    )
)

print("✅ PII masking applied")
print(f"  Rows masked : {masked.count():,}")


masked.select(
    "account_id",
    "account_number",
    "card_number",
    "iban",
    "currency"
).show(5, truncate=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Derived Columns
# 
# Add business-meaningful columns that answer the segmentation questions directly:
# - Account age → how long is the account active?
# - Activity status → is this account active, inactive, or at risk?
# - Product features → does the account have overdraft, credit card, online banking?
# - Tier label → human-readable tier description
# - Document completeness score → how complete was the onboarding?

# CELL ********************

enriched = (
    masked

    # Account age in days 
    .withColumn("account_age_days",
        F.when(
            F.col("opening_date").isNotNull(),
            F.datediff(F.current_date(), F.col("opening_date"))
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # Account age band
    .withColumn("account_age_band",
        F.when(F.col("account_age_days") < 90,   "New (<3 months)")
         .when(F.col("account_age_days") < 365,  "Recent (3-12 months)")
         .when(F.col("account_age_days") < 1095, "Established (1-3 years)")
         .when(F.col("account_age_days") >= 1095,"Mature (3+ years)")
         .otherwise("Unknown")
    )

    # Activity flags
    .withColumn("is_active",
        F.col("account_status") == "active"
    )
    .withColumn("is_inactive",
        F.col("account_status").isin("frozen", "suspended", "restricted")
    )
    .withColumn("is_at_risk",
        F.col("account_status").isin("suspended", "frozen")
    )

    # Tier label (human readable) 
    .withColumn("tier_label",
        F.when(F.col("account_tier") == "basic",    "Basic — easy/savings/joint")
         .when(F.col("account_tier") == "standard", "Standard — cheque/current/business")
         .when(F.col("account_tier") == "premium",  "Premium — gold/platinum")
         .otherwise("Unknown")
    )

    # Product features
    .withColumn("has_overdraft",
        F.col("overdraft_limit").isNotNull() & (F.col("overdraft_limit") > 0)
    )
    .withColumn("has_credit_card",
        F.col("credit_card_limit").isNotNull() & (F.col("credit_card_limit") > 0)
    )
    .withColumn("is_foreign_currency",
        F.col("currency").isin("USD", "EUR")
    )
    .withColumn("currency",                                          
        F.when(F.col("currency").isin("USD", "EUR"), F.col("currency"))
         .otherwise(F.lit("ZAR"))
    )
    .withColumn("is_joint_account",
        F.col("account_type") == "joint"
    )
    .withColumn("is_business_account",
        F.col("account_type") == "business"
    )

    # Card validity and category
    .withColumn("card_valid",
        F.when(F.col("card_expiry_date").isNull(), F.lit(None).cast(BooleanType()))
         .otherwise(F.col("card_expiry_date") > F.current_date())
    )
    .withColumn("card_category",
        F.when(F.col("card_type").contains("credit"), "credit")
         .when(F.col("card_type").contains("debit"),  "debit")
         .otherwise("none")
    )

    # Onboarding completeness score (0-7) 
    # Counts how many required documents were provided at onboarding
    .withColumn("onboarding_doc_score",
        F.coalesce(F.col("proof_of_income_provided").cast(IntegerType()),   F.lit(0)) +
        F.coalesce(F.col("proof_of_address_provided").cast(IntegerType()),  F.lit(0)) +
        F.coalesce(F.col("bank_statements_provided").cast(IntegerType()),   F.lit(0)) +
        F.coalesce(F.col("employer_letter_provided").cast(IntegerType()),   F.lit(0)) +
        F.coalesce(F.col("business_registration_provided").cast(IntegerType()), F.lit(0)) +
        F.coalesce(F.col("tax_certificate_provided").cast(IntegerType()),   F.lit(0)) +
        F.coalesce(F.col("minimum_deposit_met").cast(IntegerType()),        F.lit(0))
    )

     # Audit columns
    .withColumn("record_source", F.lit("bronze_accounts"))
    .withColumn("created_timestamp",  F.current_timestamp())
    .withColumn("updated_timestamp",  F.current_timestamp())
    .withColumn("is_current",         F.lit(True))

    # Silver metadata
    .withColumn("silver_batch_id",       F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp", F.current_timestamp())
)

print("✅ Derived columns added")
print(f"Total columns : {len(enriched.columns)}")

display(
enriched.select(
    "account_id", "account_type", "account_tier", "account_status",
    "account_age_days", "account_age_band", "tier_label",
    "is_active", "is_at_risk", "has_overdraft", "has_credit_card",
    "is_foreign_currency", "card_category", "onboarding_doc_score"
)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn(
    "currency",
    F.when(F.col("is_foreign_currency") == True,
        F.coalesce(F.col("currency"), F.lit("FOREIGN")) 
    ).otherwise(F.lit("ZAR"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

enriched = (
    enriched
    
    # days_since_status_change
    .withColumn("days_since_status_change",
    F.when(
        F.col("status_change_date").isNotNull(),
        F.datediff(F.current_date(), F.col("status_change_date"))
    ).otherwise(F.lit(None).cast(IntegerType()))
    )
    
    # approval_lag_days
    .withColumn("approval_lag_days",
    F.when(
        F.col("approval_date").isNotNull() & F.col("opening_date").isNotNull(),
        F.datediff(F.col("approval_date"), F.col("opening_date"))
    ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # card_expiring_soon
    .withColumn("card_expiring_soon",
    F.when(F.col("card_expiry_date").isNull(), F.lit(False))
     .otherwise(
        F.datediff(F.col("card_expiry_date"), F.current_date()).between(0, 90)
    )
    )
)
print("✅ Derived columns added")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  ## Primary account validation — flag customers with more than one primary account:

# CELL ********************

primary_counts = enriched.filter(F.col("is_primary_account") == True) \
    .groupBy("customer_id") \
    .agg(F.count("*").alias("primary_count"))

enriched = enriched.join(primary_counts, on="customer_id", how="left") \
    .withColumn("primary_account_violation",
        F.col("primary_count") > 1
    ).drop("primary_count")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

enriched = enriched.withColumn("approval_lag_days",
    F.when(
        F.col("approval_date").isNotNull() & F.col("opening_date").isNotNull(),
        F.datediff(F.col("approval_date"), F.col("opening_date"))
    ).otherwise(F.lit(None).cast(IntegerType()))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Write to Silver Lakehouse

# CELL ********************

def merge_silver(df, table_name, business_key, total_rows):
    """
    Upsert `df` into `table_name` using Delta MERGE on `business_key`.

    Parameters
    ----------
    df           : DataFrame — transformed Silver data
    table_name   : str       — fully-qualified Delta table name
    business_key : str       — natural key column (merge predicate)
    total_rows   : int       — pre-computed row count (avoids extra .count() action)

    Returns
    -------
    (rows_inserted, rows_updated) : tuple[int, int]
    """
    if not spark.catalog.tableExists(table_name):
        # First run — no target yet; overwrite creates the table
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        print(f"✅ Created {table_name} (first run)")
        print(f"   Inserts : {total_rows:,}")
        print(f"   Updates : 0")
        return total_rows, 0

    else:
        # Subsequent runs — Delta MERGE (upsert)
        existing_keys = spark.table(table_name).select(business_key)

        inserts = (
            df.join(existing_keys, business_key, "left_anti")
              .count()
        )
        updates = total_rows - inserts

        target = DeltaTable.forName(spark, table_name)

        # All columns updated on match except created_timestamp (preserve original)
        update_set = {
            c: f"s.{c}"
            for c in df.columns
            if c != "created_timestamp"
        }
        update_set["updated_timestamp"] = "current_timestamp()"

        (
            target.alias("t")
            .merge(df.alias("s"), f"t.{business_key} = s.{business_key}")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"✅ Merged into {table_name}")
        print(f"   Inserts : {inserts:,}")
        print(f"   Updates : {updates:,}")
        return inserts, updates


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cache enriched so the count action and the merge both read from memory
from delta.tables import DeltaTable

enriched.cache()
rows_written = enriched.count()

rows_inserted, rows_updated = merge_silver(
    enriched,
    TARGET_TABLE,
    "account_id",
    rows_written
)

print(f"\nRows processed : {rows_written:,}")
print(f"Rows inserted  : {rows_inserted:,}")
print(f"Rows updated   : {rows_updated:,}")


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
    "pipeline_name":       PIPELINE_NAME,
    "source_table":        SOURCE_TABLE,
    "watermark_column":    WATERMARK_COL,
    "watermark_value":     new_watermark,
    "batch_id":            SILVER_BATCH_ID,
    "rows_processed":      bronze_count,   
    "rows_inserted":       rows_inserted,
    "rows_updated":        rows_updated,
    "status":              "SUCCESS",
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

END_TIME = datetime.datetime.utcnow()

audit_record = spark.createDataFrame(
    [(
        PIPELINE_NAME,
        SILVER_BATCH_ID,
        SOURCE_TABLE,
        rows_written,
        rows_inserted,
        rows_updated,
        START_TIME,    # captured at notebook top — accurate batch start
        END_TIME,      # captured now — accurate batch end
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

print(f"✅ Audit log updated")
print(f"   Duration : {(END_TIME - START_TIME).total_seconds():.1f}s")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Validation Summary

# CELL ********************

dim = spark.table(TARGET_TABLE)

print("=" * 65)
print("  SILVER TRANSFORM SUMMARY — dim_accounts")
print("=" * 65)
print(f"""
  Batch ID   : {SILVER_BATCH_ID}
  Source     : {SOURCE_TABLE}
  Target     : {TARGET_TABLE}
  Rows       : {dim.count():,}
""")

print("── Account status distribution ──")
dim.groupBy("account_status").count().orderBy("count", ascending=False).show()

print("── Tier distribution ──")
dim.groupBy("account_tier", "tier_label").count().orderBy("account_tier").show()

print("── Account age band ──")
dim.groupBy("account_age_band").count().orderBy("count", ascending=False).show()

print("── Product features ──")
print(f"  Has overdraft      : {dim.filter(F.col('has_overdraft') == True).count():,}")
print(f"  Has credit card    : {dim.filter(F.col('has_credit_card') == True).count():,}")
print(f"  Foreign currency   : {dim.filter(F.col('is_foreign_currency') == True).count():,}")
print(f"  Online banking     : {dim.filter(F.col('online_banking_enabled') == True).count():,}")
print(f"  Joint accounts     : {dim.filter(F.col('is_joint_account') == True).count():,}")
print(f"  Business accounts  : {dim.filter(F.col('is_business_account') == True).count():,}")

print("\n── Onboarding doc score distribution ──")
dim.groupBy("onboarding_doc_score").count().orderBy("onboarding_doc_score").show()

print("── Card category ──")
dim.groupBy("card_category").count().orderBy("count", ascending=False).show()

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

# CELL ********************

BATCH_ID = "20260611T201545Z"
PIPELINE_NAME = "200_002_transform_accounts_silver"


spark.sql(f"""
DELETE FROM control.silver_audit_log
WHERE batch_id = '{BATCH_ID}'
AND pipeline_name = '{PIPELINE_NAME}'
""")


spark.sql(f"""
DELETE FROM control.batch_watermark
WHERE batch_id = '{BATCH_ID}'
AND pipeline_name = '{PIPELINE_NAME}'
""")

print("✅ Deleted Silver batch control records")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

tables = spark.sql("SHOW TABLES")

for row in tables.collect():
    print(row.tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }
