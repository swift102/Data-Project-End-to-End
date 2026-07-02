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
# **Notebook:** `200_001_transform_customers_silver`  
# **Source:** `lh_bronze_banking_data_modern_data.dbo.bronze_customers` (118,755 rows)  
# **Target:** `lh_silver_banking_data.customers`  
# **Layer:** Silver  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Load from Bronze + attach Silver batch metadata |
# | 2 | Enforce explicit schema — cast all columns to authoritative types |
# | 3 | Deduplicate on `customer_id` — keep latest record per customer |
# | 4 | Null classification — required vs conditionally null vs optional |
# | 5a | Pre-masking derived columns — `id_dob_match` (requires raw `id_number`) |
# | 5b | PII masking — hash/mask `id_number`, `tax_id_number`, `email`, `phone_number`, `residential_address`, `commercial_address`, `next_of_kin` |
# | 5 | Derived columns — `age`, `customer_segment`, `is_foreign_national`, `kyc_risk_tier` |
# | 6 | Split into `dim_customers_individual` + `dim_customers_business` |
# | 7 | Write to Silver Lakehouse + validation summary |
# 
# ---
# 
# ## Key Findings from Bronze Profiling
# 
# - **117,889 rows / 80,996 distinct `customer_id`** — ~37k duplicate rows from monthly snapshots
# - **2,208 nulls on core fields** (`birth_date`, `gender`, `marital_status` etc.) — same count, likely one corrupt batch
# - **Two customer types:** `Individual` (115,681 records) vs `Business` — company columns null for individuals by design
# - **`government_role`, `next_of_kin`, `guardian_customer_id`** — sparse by design, not data quality issues


# MARKDOWN ********************

# ## QUESTIONS
# 1. What is the default currency
# 2. Are bundled products going to be normalized?
# 3. Are we normalizing or keeping the data as default? Which form are we using?
# 4. What is bundled products? What is defined as a product? 

# MARKDOWN ********************

# ## 1. Configuration & Imports

# CELL ********************

import json, datetime
from pyspark.sql import functions as F
from pyspark.sql import Row
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, BooleanType, IntegerType, TimestampType
)

# Mask salt value
config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)
MASK_SALT = config["MASK_SALT"]


# Batch identity 
SILVER_BATCH_ID  = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
SILVER_LAYER     = "silver"
SOURCE_TABLE     = "lh_bronze_banking_data_modern_data.dbo.bronze_customers"
TARGET_TABLE_IND = "customers_individual"
TARGET_TABLE_BUS = "customers_non_individual" 
TARGET_DQ        = "silver_dq_customers"

print(f"Silver batch : {SILVER_BATCH_ID}")
print(f"Source       : {SOURCE_TABLE}")
print(f"Targets      : {TARGET_TABLE_IND}, {TARGET_TABLE_BUS}")


# Watermark Metadata

PIPELINE_NAME  = "200_001_transform_customers_silver"
WATERMARK_COL  = "_ingest_timestamp"

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
# Stores the last successfully processed processed_timestamp and other audit columns
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
    rows_inserted BIGINT,
    rows_updated BIGINT,
    status STRING,
    processed_timestamp TIMESTAMP
)
USING DELTA
""")

print("✅ Watermark table ready")

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

# ## 2. Load Bronze & Deduplicate
# 
# Bronze customers has **117,889 rows** but only **80,996 distinct `customer_id`** values.
# Customers appear in multiple monthly files as the dataset grows over time.
# Silver keeps the **latest record per customer** based on the source file year/month.

# CELL ********************

# Load Bronze
bronze_full = spark.table(SOURCE_TABLE)

if last_watermark is None:

    bronze = bronze_full

    print("First run detected")
    print("Loading all Bronze records")

else:

    bronze = bronze_full.filter(
        F.col("_ingest_timestamp") > F.lit(last_watermark)
    )

    print("Incremental run detected")
    print(f"Loading records after {last_watermark}")

print(f"Rows loaded : {bronze.count():,}")

# Deduplicate — keep latest record per customer_id
# Use year + month from the source file path as the recency indicator
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(
    F.col("year").desc(),
    F.col("month").desc(),
    F.col("_ingest_timestamp").desc()
)

deduped = (
    bronze
    .withColumn("_row_rank", F.row_number().over(w))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank")
)

print(f"After dedup        : {deduped.count():,}")
print(f"Duplicates removed : {bronze.count() - deduped.count():,}")


# Capture New Watermark

if bronze.count() > 0:

    new_watermark = (
        bronze
        .agg(F.max("_ingest_timestamp"))
        .collect()[0][0]
    )

else:

    new_watermark = last_watermark

print(f"New watermark : {new_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Enforce Explicit Schema
# 
# Bronze uses `inferSchema` which produces inconsistencies across monthly files.
# Silver enforces authoritative types for every column.A

# CELL ********************

typed = (
    deduped
    # Identity & classification
    .withColumn("customer_id",            F.col("customer_id").cast(StringType()))
    .withColumn("customer_type",          F.trim(F.col("customer_type")).cast(StringType()))

    # Personal information 
    .withColumn("full_name",              F.trim(F.col("full_name")).cast(StringType()))
    .withColumn("birth_date",             F.col("birth_date").cast(DateType()))
    .withColumn("gender",                 F.trim(F.upper(F.col("gender"))).cast(StringType()))
    .withColumn("marital_status",         F.trim(F.col("marital_status")).cast(StringType()))
    .withColumn("ethnicity",              F.trim(F.col("ethnicity")).cast(StringType()))
    .withColumn("education_level",        F.trim(F.col("education_level")).cast(StringType()))
    .withColumn("occupation",             F.trim(F.col("occupation")).cast(StringType()))
    .withColumn("employer_name",          F.trim(F.col("employer_name")).cast(StringType()))
    .withColumn("annual_income",          F.col("annual_income").cast(DoubleType()))

    # Contact & address 
    .withColumn("residential_address",    F.trim(F.col("residential_address")).cast(StringType()))
    .withColumn("residential_postal_code",F.trim(F.col("residential_postal_code")).cast(StringType()))
    .withColumn("commercial_address",     F.trim(F.col("commercial_address")).cast(StringType()))
    .withColumn("email",                  F.trim(F.lower(F.col("email"))).cast(StringType()))
    .withColumn("phone_number",           F.trim(F.col("phone_number")).cast(StringType()))
    .withColumn("preferred_contact_method",F.trim(F.col("preferred_contact_method")).cast(StringType()))

    # Identification documents 
    .withColumn("id_type",                F.trim(F.col("id_type")).cast(StringType()))
    .withColumn("id_number",              F.trim(F.col("id_number")).cast(StringType()))
    .withColumn("expiry_date",            F.col("expiry_date").cast(DateType()))
    .withColumn("passport_expired",       F.col("passport_expired").cast(BooleanType()))
    .withColumn("visa_type",              F.trim(F.col("visa_type")).cast(StringType()))
    .withColumn("visa_expiry_date",       F.col("visa_expiry_date").cast(DateType()))
    .withColumn("citizenship",            F.trim(F.upper(F.col("citizenship"))).cast(StringType()))
    .withColumn("nationality",            F.trim(F.col("nationality")).cast(StringType()))
    .withColumn("tax_id_number",          F.trim(F.col("tax_id_number")).cast(StringType()))
    .withColumn("date_of_entry",          F.col("date_of_entry").cast(DateType()))

    # Risk & compliance 
    .withColumn("risk_score",             F.col("risk_score").cast(DoubleType()))
    .withColumn("is_pep",                 F.col("is_pep").cast(BooleanType()))
    .withColumn("sanctioned_country",     F.col("sanctioned_country").cast(BooleanType()))
    .withColumn("is_government_official", F.col("is_government_official").cast(BooleanType()))
    .withColumn("government_role",        F.trim(F.col("government_role")).cast(StringType()))
    .withColumn("source_of_funds",        F.trim(F.col("source_of_funds")).cast(StringType()))
    .withColumn("location_exposure",      F.trim(F.col("location_exposure")).cast(StringType()))

    # Digital & behavioural 
    .withColumn("financial_goal",         F.trim(F.col("financial_goal")).cast(StringType()))
    .withColumn("device_type",            F.trim(F.col("device_type")).cast(StringType()))
    .withColumn("capture_channel",        F.trim(F.col("capture_channel")).cast(StringType()))

    # Branch
    .withColumn("branch_id",              F.trim(F.col("branch_id")).cast(StringType()))
    .withColumn("branch_name",            F.trim(F.col("branch_name")).cast(StringType()))
    .withColumn("branch_city",            F.trim(F.col("branch_city")).cast(StringType()))
    .withColumn("branch_province",        F.trim(F.col("branch_province")).cast(StringType()))

    # Business-only fields 
    .withColumn("company_age",            F.col("company_age").cast(DoubleType()))
    .withColumn("company_size",           F.trim(F.col("company_size")).cast(StringType()))
    .withColumn("number_of_employees",    F.col("number_of_employees").cast(IntegerType()))
    .withColumn("annual_turnover",        F.col("annual_turnover").cast(DoubleType()))
    .withColumn("directors_count",        F.col("directors_count").cast(IntegerType()))
    .withColumn("shareholders_count",     F.col("shareholders_count").cast(IntegerType()))
    .withColumn("beneficial_owners_count",F.col("beneficial_owners_count").cast(IntegerType()))
    .withColumn("bee_level",              F.col("bee_level").cast(IntegerType()))
    .withColumn("vat_registered",         F.col("vat_registered").cast(BooleanType()))
    .withColumn("industry_risk_rating",   F.trim(F.col("industry_risk_rating")).cast(StringType()))

    # Other
    .withColumn("next_of_kin",            F.trim(F.col("next_of_kin")).cast(StringType()))
    .withColumn("guardian_customer_id",   F.trim(F.col("guardian_customer_id")).cast(StringType()))
    .withColumn("is_affidavit",           F.col("is_affidavit").cast(BooleanType()))
)

print("✅ Schema enforced")
typed.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Null Classification & Required Field Validation
# 
# Not all nulls are equal. This step separates:
# - **Required nulls** — fields that must never be null (fail = data quality issue)
# - **Conditional nulls** — null is valid only for a specific `customer_type`
# - **Optional nulls** — sparse by design (e.g. `next_of_kin`, `government_role`)

# CELL ********************

# Required fields — must never be null 
REQUIRED = ["customer_id", "customer_type", "full_name", "risk_score", "branch_id"]

# Conditional — null only valid for Business customers
INDIVIDUAL_REQUIRED = ["birth_date", "gender", "marital_status", "id_type",
                       "id_number", "citizenship", "nationality", "annual_income",
                       "education_level", "ethnicity", "location_exposure", "financial_goal"]

# Conditional — null only valid for Individual customers
BUSINESS_REQUIRED   = ["company_size", "annual_turnover", "directors_count",
                       "shareholders_count", "vat_registered"]

print("=" * 60)
print("REQUIRED FIELD NULL CHECK")
print("=" * 60)

dq_issues = []

for col in REQUIRED:
    n = typed.filter(F.col(col).isNull()).count()
    status = "✅" if n == 0 else "❌ FAIL"
    print(f"  {status}  {col}: {n:,} nulls")
    if n > 0:
        dq_issues.append({"field": col, "issue": "required_null", "count": n})

print("\nINDIVIDUAL-ONLY FIELD NULL CHECK (filtered to Individual rows)")
ind = typed.filter(F.col("customer_type") == "Individual")
for col in INDIVIDUAL_REQUIRED:
    if col in typed.columns:
        n = ind.filter(F.col(col).isNull()).count()
        status = "✅" if n == 0 else f"⚠️  {n:,} nulls"
        print(f"  {status}  {col}")

print("\nBUSINESS-ONLY FIELD NULL CHECK (filtered to Business rows)")
bus = typed.filter(F.col("customer_type") != "Individual")
for col in BUSINESS_REQUIRED:
    if col in typed.columns:
        n = bus.filter(F.col(col).isNull()).count()
        status = "✅" if n == 0 else f"⚠️  {n:,} nulls"
        print(f"  {status}  {col}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DQ-CUSTOMERS-001 
# ~2,811 Individual records have birth_date offset by exactly ±80 years.
# Pattern: future dates (negative age) and ancient dates (age > 120).
# Fix: 80-year correction applied here. Original preserved temporarily in birth_date_raw.
# Flagged with dq_birth_date_corrected=True and dq_birth_date_suspect=True
# (suspect = still outside valid range after correction).
# Only applied to Individual rows — Company and Organization have null birth_date by design.
# Action: Confirm offset pattern with source system owner before dropping birth_date_raw.


OFFSET_MONTHS = 960   # 80 years
MIN_VALID_AGE = 0
MAX_VALID_AGE = 120

# Step 1: compute raw age — Individual only
df_age = typed.withColumn(
    "age_raw",
    F.when(
        F.col("customer_type") == "Individual",
        F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12)
    ).otherwise(F.lit(None).cast("double"))
)

# Step 2: apply the 80-year correction — Individual only
# Future dates (negative age)  → subtract 80 years
# Ancient dates (age > 120)    → add 80 years
# Everything else              → leave alone
df_fixed = df_age.withColumn(
    "birth_date_raw",
    F.col("birth_date")
).withColumn(
    "birth_date",
    F.when(
        (F.col("customer_type") == "Individual") & (F.col("age_raw") < MIN_VALID_AGE),
        F.add_months("birth_date", -OFFSET_MONTHS)
    ).when(
        (F.col("customer_type") == "Individual") & (F.col("age_raw") > MAX_VALID_AGE),
        F.add_months("birth_date", OFFSET_MONTHS)
    ).otherwise(F.col("birth_date"))
)

# Step 3: recompute age on corrected date — Individual only
df_fixed = df_fixed.withColumn(
    "age_corrected",
    F.when(
        F.col("customer_type") == "Individual",
        F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12)
    ).otherwise(F.lit(None).cast("double"))
)

# Step 4: DQ flags
df_fixed = df_fixed.withColumn(
    "dq_birth_date_suspect",
    F.when(
        (F.col("customer_type") == "Individual") &
        (
            (F.col("age_corrected") < MIN_VALID_AGE) |
            (F.col("age_corrected") > MAX_VALID_AGE)
        ),
        F.lit(True)
    ).otherwise(F.lit(False))
).withColumn(
    "dq_birth_date_corrected",
    F.when(
        (F.col("customer_type") == "Individual") &
        (F.col("birth_date_raw") != F.col("birth_date")),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# Step 5: sanity check — all three types should appear; Company + Organization must show 0
df_fixed.groupBy("customer_type").agg(
    F.sum(F.when(F.col("dq_birth_date_corrected"), 1).otherwise(0)).alias("corrected_count"),
    F.sum(F.when(F.col("dq_birth_date_suspect"), 1).otherwise(0)).alias("still_suspect"),
).orderBy("customer_type").show()

# Drop working columns — birth_date_raw is temporary, not a Silver column
typed = df_fixed.drop("age_raw", "age_corrected", "birth_date_raw")
# dq_birth_date_corrected and dq_birth_date_suspect are kept as Silver columns
print("✅ Birth date correction applied (DQ-CUSTOMERS-001)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Masking Strategy
# 
# | Column | Technique | Reason |
# |---|---|---|
# | `id_number` | Deterministic hash (SHA-256 + salt) | Join key in KYC systems — must be consistent across runs |
# | `tax_id_number` | Deterministic hash (SHA-256 + salt) | Sensitive identifier, not a pipeline join key |
# | `email` | Domain-preserving hash | Local part hashed, domain kept for analytics (gmail vs corporate) |
# | `phone_number` | Partial mask — keep prefix + last 3 digits | Not a join key; country code retained for analytics |
# | `residential_address` | Full hash | Free text, no structure worth preserving |
# | `commercial_address` | Full hash | Free text, no structure worth preserving |
# | `next_of_kin` | Full hash | Free text name, not used anywhere downstream |

# MARKDOWN ********************

# ## Pre-Masking Derived Column — `id_dob_match`
# 
# `id_dob_match` must run **before** masking because it reads the first 6 characters
# of the raw SA ID number to validate the embedded date-of-birth.
# Once `id_number` is hashed it becomes a 64-character hex string — the substring
# check would silently return `False` for every row instead of raising an error.

# CELL ********************

"""
South African National IDs embed the holder's date of birth in the first 6 digits:
format YYMMDD. e.g. ID 8001015009087 → first 6 = "800101" = 1980-01-01.
We extract those 6 chars and compare to birth_date formatted the same way.
This check is only meaningful for National ID type like passport and other IDs
don't follow this convention, so we guard with id_type == "National ID".

"""

typed = typed.withColumn("id_dob_match",
    F.when(
        (F.col("id_type") == "National ID") &
        F.col("id_number").isNotNull() &
        F.col("birth_date").isNotNull(),
        F.substring(F.col("id_number"), 1, 6) ==
        F.date_format(F.col("birth_date"), "yyMMdd")
    ).otherwise(F.lit(None).cast(BooleanType()))
)

match_count = typed.filter(F.col("id_dob_match") == True).count()
mismatch_count = typed.filter(F.col("id_dob_match") == False).count()

print("✅ id_dob_match on raw id_number")
print(f"  Match    : {match_count:,}")
print(f"  Mismatch : {mismatch_count:,}  ← investigate if unexpectedly high")
print(f"  Null     : {typed.filter(F.col('id_dob_match').isNull()).count():,}  (non-National ID or missing fields)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PII Masking
# 
# Bronze retains the raw values. Silver is the first layer downstream consumers touch.
# 
# **Techniques used:**
# - **Deterministic hash** `sha2(concat(value, salt), 256)` — for structured IDs and keys
# - **Domain-preserving hash** — for email: hash local part, keep domain
# - **Partial mask** — for phone: keep country prefix + last 3 digits, replace middle with `***`
# - **Full hash** — for free-text fields (addresses, next of kin)


# CELL ********************

masked = (
    typed

    # Deterministic hash: id_number
    # South African ID / passport number — most sensitive field.
    # SHA-256 produces a 64-char hex string. Salt prevents rainbow table attacks.
    # isNotNull() guard ensures nulls stay null — hashing None would produce
    # a valid-looking hash of the string "None", poisoning downstream checks.
   .withColumn("id_number",
        F.when(
            F.col("id_number").isNotNull(),
            F.sha2(F.concat(F.col("id_number"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Deterministic hash: tax_id_number
    # Same treatment as id_number. Not a pipeline join key but still sensitive.
    .withColumn("tax_id_number",
        F.when(
            F.col("tax_id_number").isNotNull(),
            F.sha2(F.concat(F.col("tax_id_number"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Domain-preserving hash: email
    # Split on "@" — hash only the local part (before @), keep the domain.
    # Why keep the domain? It's analytically useful (gmail vs corporate) and
    # not personally identifying on its own.
    # The contains("@") guard protects malformed emails — those become null,
    # which is correct since they were invalid to begin with.
    # is_valid_email and is_duplicate_email run AFTER this on the masked value —
    # both still work correctly because:
    #   - is_valid_email: masked local part still sits left of "@", regex passes
    #   - is_duplicate_email: hashing is deterministic so duplicates hash identically
    .withColumn("email",
        F.when(
            F.col("email").isNotNull() & F.col("email").contains("@"),
            F.concat(
                F.sha2(
                    F.concat(F.split(F.col("email"), "@")[0], F.lit(MASK_SALT)),                  # email should be <64-char-hex>@<domain>
                    256
                ),
                F.lit("@"),
                F.split(F.col("email"), "@")[1]
            )
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Partial mask: phone_number 
    # Keeps the country code prefix (first 4 chars e.g. "+278") and
    # the last 3 digits. Middle digits replaced with "***".
    # Why not hash? phone_number isn't used as a join key in this pipeline —
    # partial masking is sufficient and retains country-code analytics via
    # the phone_country_code derived column (which runs after this on the
    # masked value — "+278" prefix is still intact so country detection works).
    .withColumn("phone_number",
        F.when(
            F.col("phone_number").isNotNull(),
            F.concat(
                F.substring(F.col("phone_number"), 1, 4),
                F.lit("***"),
                F.substring(F.col("phone_number"), -3, 3)
            )
        ).otherwise(F.lit(None).cast(StringType()))                  # phone should be +XXX***NNN
    )

    # Full hash: residential_address
    # Free-text field — no structure worth preserving.
    # Not used as a join key. Full hash removes all geographic information.
    .withColumn("residential_address",
        F.when(
            F.col("residential_address").isNotNull(),
            F.sha2(F.concat(F.col("residential_address"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Full hash: commercial_address
    .withColumn("commercial_address",
        F.when(
            F.col("commercial_address").isNotNull(),
            F.sha2(F.concat(F.col("commercial_address"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Full hash: next_of_kin 
    # Free-text name field. Not used downstream. Full hash.
    .withColumn("next_of_kin",
        F.when(
            F.col("next_of_kin").isNotNull(),
            F.sha2(F.concat(F.col("next_of_kin"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # ── Carry pre-masking flag through ────────────────────────────────────
    # id_dob_match was computed on typed (unmasked id_number) in the above Cell for pre-masking.
    # It must be explicitly carried into masked, otherwise it is dropped
    # when masked is constructed from typed without it.
    .withColumn("id_dob_match", F.col("id_dob_match"))
)

print("✅ PII masking applied")
print(f"  Rows masked : {masked.count():,}")

masked.select(
    "customer_id",
    "id_number",
    "tax_id_number",
    "email",
    "phone_number",
    "residential_address"
).show(5, truncate=True)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Derived Columns
# 
# Add business-meaningful columns that will power customer segmentation in the Gold layer.

# CELL ********************

enriched = (
    masked

    # Age (years)
    .withColumn("age",
        F.when(
            F.col("birth_date").isNotNull(),
            F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
        ).otherwise(F.lit(None).cast(IntegerType()))
    )

    # Age band (for segmentation) 
    .withColumn("age_band",
        F.when(F.col("age") < 25,  "18-24")
         .when(F.col("age") < 35,  "25-34")
         .when(F.col("age") < 45,  "35-44")
         .when(F.col("age") < 55,  "45-54")
         .when(F.col("age") < 65,  "55-64")
         .when(F.col("age") >= 65, "65+")
         .otherwise("Unknown")
    )

    # Foreign national flag 
    .withColumn("is_foreign_national",
        F.when(F.col("citizenship").isin("ZA", "South Africa"), F.lit(False))
         .when(F.col("citizenship").isNull(), F.lit(None).cast(BooleanType()))
         .otherwise(F.lit(True))
    )

    # Citizenship
    .withColumn("is_dual_citizen",
     F.when(F.col("citizenship").contains(","), True).otherwise(False).cast(BooleanType()))

    .withColumn("primary_citizenship",
      F.trim(F.split(F.col("citizenship"), ",")[0]).cast(StringType()))

    # KYC risk tier
    .withColumn("kyc_risk_tier",
        F.when(F.col("risk_score") < 0.3,  "Low")
         .when(F.col("risk_score") < 0.6,  "Medium")
         .when(F.col("risk_score") < 0.8,  "High")
         .when(F.col("risk_score") >= 0.8, "Critical")
         .otherwise("Unscored")
    )

    # High risk flag (PEP or sanctioned or critical score) 
    .withColumn("is_high_risk",
        (F.col("is_pep") == True) |
        (F.col("sanctioned_country") == True) |
        (F.col("kyc_risk_tier") == "Critical")
    )

    # Income band
    .withColumn("income_band",
        F.when(F.col("annual_income").isNull(),      "Unknown")
         .when(F.col("annual_income") < 50000,       "Low (<50k)")
         .when(F.col("annual_income") < 200000,      "Lower-Middle (50k-200k)")
         .when(F.col("annual_income") < 500000,      "Middle (200k-500k)")
         .when(F.col("annual_income") < 1000000,     "Upper-Middle (500k-1M)")
         .otherwise(                                  "High (1M+)")
    )

    # Passport validity
    .withColumn("passport_valid",
        F.when(F.col("id_type") != "Passport", F.lit(None).cast(BooleanType()))
         .when(F.col("expiry_date").isNull(),   F.lit(None).cast(BooleanType()))
         .otherwise(F.col("expiry_date") > F.current_date())
    )

    # Visa validity
    .withColumn("visa_valid",
        F.when(F.col("visa_expiry_date").isNull(), F.lit(None).cast(BooleanType()))
         .otherwise(F.col("visa_expiry_date") > F.current_date())
    )

    # Email format validation
    # runs on masked email — masked local part still satisfies the regex
    #because sha2() produces hex chars [0-9a-f] which match [a-zA-Z0-9._%+-]

    .withColumn("is_valid_email",
        F.when(
            F.col("email").isNull(),
            F.lit(None).cast(BooleanType())
        ).otherwise(
            F.col("email").rlike(
                r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
            )
        )
    )

    # Phone country code extraction
    # Works correctly on masked phone — partial mask preserves the prefix (+278 etc.)
    .withColumn("phone_country_code",
        F.when(F.col("phone_number").startswith("+27"),  "ZA")
         .when(F.col("phone_number").startswith("+254"), "KE")
         .when(F.col("phone_number").startswith("+266"), "LS")
         .when(F.col("phone_number").startswith("+263"), "ZW")
         .when(F.col("phone_number").isNull(), F.lit(None))
         .otherwise("OTHER")
    )

    # id_dob_match is already present — computed on raw id_number in Step 5a
    # Do NOT recompute here

    # Audit columns
    .withColumn("record_source",      F.lit("bronze_customers"))
    .withColumn("created_timestamp",  F.current_timestamp())
    .withColumn("updated_timestamp",  F.current_timestamp())
    .withColumn("is_current",         F.lit(True))

    # Silver metadata 
    .withColumn("silver_batch_id",        F.lit(SILVER_BATCH_ID))
    .withColumn("silver_load_timestamp",  F.current_timestamp())
)

print("✅ Derived columns added")
print(f"Total columns : {len(enriched.columns)}")
print(f"  is_valid_email — true : {enriched.filter(F.col('is_valid_email') == True).count():,}")
print(f"                  false : {enriched.filter(F.col('is_valid_email') == False).count():,}")
print(f"                  null  : {enriched.filter(F.col('is_valid_email').isNull()).count():,}")

# Verify derived columns
enriched.select(
    "customer_id", "customer_type", "age", "age_band",
    "kyc_risk_tier", "is_high_risk", "income_band",
    "is_foreign_national", "passport_valid", "visa_valid",
    "id_dob_match", "is_valid_email"         
).show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Customer Segmentation Features

enriched = (
    enriched

    # Customer surrogate key
    .withColumn(
        "customer_sk",
        F.xxhash64("customer_id")
    )

    # Customer tenure (using first appearance in source)
    .withColumn(
        "customer_tenure_years",
        F.floor(
            F.datediff(
                F.current_date(),
                F.to_date("_ingest_timestamp")
            ) / 365.25
        )
    )

    .withColumn(
        "tenure_band",
        F.when(F.col("customer_tenure_years") < 1, "New")
         .when(F.col("customer_tenure_years") < 3, "Growing")
         .when(F.col("customer_tenure_years") < 5, "Established")
         .otherwise("Loyal")
    )

    # Digital customer flag
    .withColumn(
        "is_digital_customer",
        F.when(
            F.lower(F.col("capture_channel")).isin(
                "mobile app",
                "online banking",
                "web",
                "internet banking"
            ),
            True
        ).otherwise(False)
    )

    # Simple risk segment
    .withColumn(
        "risk_segment",
        F.when(F.col("is_high_risk"), "High Risk")
         .otherwise("Standard Risk")
    )

    # Income + Risk profile
    .withColumn(
        "income_risk_profile",
        F.concat_ws(
            "_",
            F.col("income_band"),
            F.col("kyc_risk_tier")
        )
    )
)

print("✅ Customer segmentation features added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Customer Completeness Score

enriched = (
    enriched

    .withColumn(
        "completeness_score",

        F.when(F.col("email").isNotNull(), 1).otherwise(0) +
        F.when(F.col("phone_number").isNotNull(), 1).otherwise(0) +
        F.when(F.col("id_number").isNotNull(), 1).otherwise(0) +
        F.when(F.col("residential_address").isNotNull(), 1).otherwise(0) +
        F.when(F.col("annual_income").isNotNull(), 1).otherwise(0) +
        F.when(F.col("risk_score").isNotNull(), 1).otherwise(0)
    )

    .withColumn(
        "completeness_band",
        F.when(F.col("completeness_score") <= 2, "Poor")
         .when(F.col("completeness_score") <= 4, "Moderate")
         .otherwise("Complete")
    )
)

print("✅ Completeness score added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Retail Banking Customer Segment

enriched = (
    enriched

    .withColumn(
        "customer_segment",

        F.when(
            (F.col("customer_type") == "Individual") &
            (F.col("annual_income") >= 1000000),
            "Affluent"
        )

        .when(
            (F.col("customer_type") == "Individual") &
            (F.col("annual_income") >= 200000),
            "Mass Market"
        )

        .when(
            (F.col("customer_type") == "Individual"),
            "Entry Level"
        )

        .otherwise(None)
    )
)
print("✅ Retail customer segments added")


# Business Banking Segment

enriched = (
    enriched

    .withColumn(
        "business_segment",

        F.when(
            F.col("annual_turnover") < 1000000,
            "Small Business"
        )

        .when(
            F.col("annual_turnover") < 10000000,
            "Medium Business"
        )

        .when(
            F.col("annual_turnover").isNotNull(),
            "Large Enterprise"
        )

        .otherwise(None)
    )
)

print("✅ Business customer segments added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Segmentation Readiness Flag

enriched = (
    enriched

    .withColumn(
        "segmentation_ready",

        (
            F.col("risk_score").isNotNull()
            & F.col("citizenship").isNotNull()
            & F.col("customer_type").isNotNull()
        )
    )
)

print("✅ Segmentation readiness flag added")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Split by Customer Type
# 
# Individuals and Businesses have fundamentally different schemas.
# Keeping them in one table forces all business columns to be null for individuals and vice versa.
# Silver splits them into two clean tables.

# CELL ********************

# Columns relevant to Individual customers
IND_COLS = [
    "customer_id",
    "customer_sk",
    "customer_type",
    "full_name",

    # Personal
    "birth_date",
    "age",
    "age_band",
    "gender",
    "marital_status",
    "ethnicity",
    "education_level",
    "occupation",
    "employer_name",

    # Financial
    "annual_income",
    "income_band",
    "source_of_funds",

    # Segmentation
    "customer_tenure_years",
    "tenure_band",
    "customer_segment",
    "risk_segment",
    "income_risk_profile",
    "is_digital_customer",
    "completeness_score",
    "completeness_band",
    "segmentation_ready",

    # Citizenship
    "citizenship",
    "nationality",
    "is_foreign_national",
    "primary_citizenship",
    "is_dual_citizen",

    # Identity
    "id_type",
    "id_number",
    "expiry_date",
    "passport_expired",
    "passport_valid",
    "visa_type",
    "visa_expiry_date",
    "visa_valid",
    "tax_id_number",
    "date_of_entry",

    # Contact
    "residential_address",
    "residential_postal_code",
    "commercial_address",
    "email",
    "phone_number",
    "preferred_contact_method",

    # Risk
    "risk_score",
    "kyc_risk_tier",
    "is_high_risk",
    "is_pep",
    "sanctioned_country",
    "is_government_official",
    "government_role",

    # Behavioural
    "location_exposure",
    "financial_goal",
    "device_type",
    "capture_channel",

    # Other
    "next_of_kin",
    "guardian_customer_id",
    "is_affidavit",

    # Branch
    "branch_id",
    "branch_name",
    "branch_city",
    "branch_province",

    # DQ flags
    "dq_birth_date_corrected",
    "dq_birth_date_suspect",

    # Audit
    "record_source",
    "created_timestamp",
    "updated_timestamp",
    "is_current",

    # Metadata
    "silver_batch_id",
    "silver_load_timestamp",
    "_source_file",
    "_ingest_timestamp",
    "_batch_id",
    "_commit_sha"
]

# Columns relevant to Business customers
BUS_COLS = [
    "customer_id",
    "customer_sk",
    "customer_type",
    "full_name",

    # Segmentation
    "customer_tenure_years",
    "tenure_band",
    "business_segment",
    "risk_segment",
    "income_risk_profile",
    "is_digital_customer",
    "completeness_score",
    "completeness_band",
    "segmentation_ready",

    # Registration
    "citizenship",
    "nationality",
    "tax_id_number",

    # Contact
    "residential_address",
    "residential_postal_code",
    "commercial_address",
    "email",
    "phone_number",
    "preferred_contact_method",

    # Business
    "source_of_funds",
    "capture_channel",
    "company_age",
    "company_size",
    "number_of_employees",
    "annual_turnover",
    "directors_count",
    "shareholders_count",
    "beneficial_owners_count",
    "bee_level",
    "vat_registered",
    "industry_risk_rating",

    # Risk
    "risk_score",
    "kyc_risk_tier",
    "is_high_risk",
    "is_pep",
    "sanctioned_country",
    "is_government_official",
    "government_role",
    "location_exposure",

    # Other
    "is_affidavit",

    # Branch
    "branch_id",
    "branch_name",
    "branch_city",
    "branch_province",

    # DQ flags
    "dq_birth_date_corrected",
    "dq_birth_date_suspect",

    # Audit
    "record_source",
    "created_timestamp",
    "updated_timestamp",
    "is_current",

    # Metadata
    "silver_batch_id",
    "silver_load_timestamp",
    "_source_file",
    "_ingest_timestamp",
    "_batch_id",
    "_commit_sha"
]

# Create dimensions
individual = (
    enriched
    .filter(F.col("customer_type") == "Individual")
    .select(IND_COLS)
)

non_individual = (
    enriched
    .filter(F.col("customer_type").isin("Company", "Organization"))
    .select(BUS_COLS)
)

print(f"customers_individual     : {individual.count():,} rows")
print(f"customers_non_individual : {non_individual.count():,} rows")
print(f"Total                    : {(individual.count() + non_individual.count()):,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  Duplicate email flag

# CELL ********************

email_counts = individual.groupBy("email").agg(
    F.count("*").alias("email_count")
).filter(F.col("email").isNotNull())

individual = individual.join(email_counts, on="email", how="left") \
    .withColumn("is_duplicate_email", F.col("email_count") > 1) \
    .drop("email_count")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_inserted = individual.count() + non_individual.count()
rows_updated  = 0  # first run; merge_silver tracks this in subsequent runs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Write to Silver Lakehouse

# CELL ********************

from delta.tables import DeltaTable

def merge_silver(df, table_name, business_key):

    # First load
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
        print(f"Inserts : {inserts:,}")
        print(f"Updates : {updates:,}")

        return inserts, updates

    # Existing records
    existing_keys = (
        spark.table(table_name)
        .select(business_key)
    )

    inserts = (
        df.join(
            existing_keys,
            business_key,
            "left_anti"
        ).count()
    )

    updates = df.count() - inserts

    target = DeltaTable.forName(
        spark,
        table_name
    )

    update_set = {
        c: f"s.{c}"
        for c in df.columns
        if c != "created_timestamp"
    }

    update_set["updated_timestamp"] = "current_timestamp()"

    (
        target.alias("t")
        .merge(
            df.alias("s"),
            f"t.{business_key} = s.{business_key}"
        )
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged {table_name}")
    print(f"Inserts : {inserts:,}")
    print(f"Updates : {updates:,}")

    return inserts, updates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ind_inserts, ind_updates = merge_silver(
    individual,
    TARGET_TABLE_IND,
    "customer_id"
)

bus_inserts, bus_updates = merge_silver(
    non_individual,
    TARGET_TABLE_BUS,
    "customer_id"
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_inserted = ind_inserts + bus_inserts
rows_updated = ind_updates + bus_updates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Update Watermark

# CELL ********************

rows_written = (
    individual.count()
    + non_individual.count()
)


watermark_record = spark.createDataFrame(
    [
        (
            PIPELINE_NAME,
            SOURCE_TABLE,
            WATERMARK_COL,
            new_watermark,
            SILVER_BATCH_ID,
            rows_written,
            rows_inserted,
            rows_updated,
            "SUCCESS",
            datetime.datetime.utcnow()
        )
    ],
    """
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
    """
)

(
    watermark_record.write
    .format("delta")
    .mode("append")
    .saveAsTable("control.batch_watermark")
)

print("✅ Watermark updated")  
display("control.batch_watermark")

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

# ## 8. Data Quality Summary

# CELL ********************

print("=" * 65)
print("  SILVER TRANSFORM SUMMARY — dim_customers")
print("=" * 65)
print(f"""
  Batch ID     : {SILVER_BATCH_ID}
  Source       : {SOURCE_TABLE}

  DEDUPLICATION
  Bronze rows         : {bronze.count():,}
  Silver rows (total) : {individual.count() + non_individual.count():,}
  Duplicates removed  : {bronze.count() - (individual.count() + non_individual.count()):,}

  OUTPUT TABLES
  customers_individual     : {individual.count():,} rows
  customers_non_individual : {non_individual.count():,} rows

""")
print(f"""
MERGE STATISTICS

Rows Processed : {rows_written:,}
Rows Inserted  : {rows_inserted:,}
Rows Updated   : {rows_updated:,}
""")
print(f"""
WATERMARK CONTROL

Previous Watermark : {last_watermark}
New Watermark      : {new_watermark}
Pipeline           : {PIPELINE_NAME}
""")
print("=" * 65)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Segmentation Preview
# 
# Quick look at the customer segments that will feed the Gold layer.

# CELL ********************

ind = spark.table(TARGET_TABLE_IND)

print("KYC Risk Tier Distribution")
ind.groupBy("kyc_risk_tier").count().orderBy("count", ascending=False).show()

print("Age Band Distribution")
ind.groupBy("age_band").count().orderBy("age_band").show()

print("Income Band Distribution")
ind.groupBy("income_band").count().orderBy("count", ascending=False).show()

print("Branch Province Distribution")
ind.groupBy("branch_province").count().orderBy("count", ascending=False).show()

print("High Risk Customers")
print(f"  PEP customers            : {ind.filter(F.col('is_pep') == True).count():,}")
print(f"  Sanctioned country       : {ind.filter(F.col('sanctioned_country') == True).count():,}")
print(f"  Critical risk score      : {ind.filter(F.col('kyc_risk_tier') == 'Critical').count():,}")
print(f"  Total high risk          : {ind.filter(F.col('is_high_risk') == True).count():,}")
print(f"  Foreign nationals        : {ind.filter(F.col('is_foreign_national') == True).count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## The below cells are there to test validation of the data and should not be implemented as part of the routine pipeline run:
# - email validation findings
# - duplicate detection

# CELL ********************

from pyspark.sql import functions as F

ind = spark.table("customers_individual")

# ── Email shape overview ───────────────────────────────────────────────────
total        = ind.count()
null_email   = ind.filter(F.col("email").isNull()).count()
has_email    = ind.filter(F.col("email").isNotNull()).count()

# Basic @ check on masked email
has_at       = ind.filter(F.col("email").contains("@")).count()
missing_at   = ind.filter(F.col("email").isNotNull() & ~F.col("email").contains("@")).count()

# Domain shape
ind.filter(F.col("email").isNotNull()) \
   .withColumn("domain", F.split(F.col("email"), "@")[1]) \
   .groupBy("domain") \
   .count() \
   .orderBy("count", ascending=False) \
   .show(15, truncate=False)

print(f"Total rows       : {total:,}")
print(f"Null email       : {null_email:,}")
print(f"Has email        : {has_email:,}")
print(f"Has @ symbol     : {has_at:,}")
print(f"Missing @ symbol : {missing_at:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Regex: <something>@<something>.<2-6 char TLD>
EMAIL_REGEX = r'^[^@]+@[^@]+\.[a-zA-Z]{2,6}$'

ind.filter(F.col("email").isNotNull()) \
   .withColumn("is_valid_email",
       F.col("email").rlike(EMAIL_REGEX)
   ) \
   .groupBy("is_valid_email") \
   .count() \
   .orderBy("is_valid_email") \
   .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

ind = spark.table("customers_individual")

# ── 1. Duplicate hashed email ─────────────────────────────────────────────
print("=== Duplicate email (hashed) ===")
ind.filter(F.col("email").isNotNull()) \
   .groupBy("email") \
   .count() \
   .filter(F.col("count") > 1) \
   .agg(
       F.count("*").alias("duplicate_email_groups"),
       F.sum("count").alias("customers_in_duplicate_groups")
   ).show()

# ── 2. Duplicate hashed id_number ─────────────────────────────────────────
print("=== Duplicate id_number (hashed) ===")
ind.filter(F.col("id_number").isNotNull()) \
   .groupBy("id_number") \
   .count() \
   .filter(F.col("count") > 1) \
   .agg(
       F.count("*").alias("duplicate_id_groups"),
       F.sum("count").alias("customers_in_duplicate_groups")
   ).show()

# ── 3. Duplicate hashed tax_id_number ────────────────────────────────────
print("=== Duplicate tax_id_number (hashed) ===")
ind.filter(F.col("tax_id_number").isNotNull()) \
   .groupBy("tax_id_number") \
   .count() \
   .filter(F.col("count") > 1) \
   .agg(
       F.count("*").alias("duplicate_tax_id_groups"),
       F.sum("count").alias("customers_in_duplicate_groups")
   ).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

ind.groupBy("is_duplicate_email").count().orderBy("is_duplicate_email").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

ind.filter(F.col("id_number").isNotNull()) \
   .groupBy("id_number") \
   .count() \
   .filter(F.col("count") > 1) \
   .join(ind.select("customer_id", "id_number"), on="id_number") \
   .select("id_number", "customer_id") \
   .show(truncate=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Delete customers row from both control tables

spark.sql("""
    DELETE FROM lh_silver_banking_data.control.batch_watermark
    WHERE pipeline_name = '200_001_transform_customers_silver'
""")

spark.sql("""
    DELETE FROM lh_silver_banking_data.control.silver_audit_log
    WHERE pipeline_name = '200_001_transform_customers_silver'
""")

print("✅ Deleted customers rows from both control tables")

# Verify
spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_silver_banking_data.control.batch_watermark
    WHERE pipeline_name = '200_001_transform_customers_silver'
""").show(truncate=False)

spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_silver_banking_data.control.silver_audit_log
    WHERE pipeline_name = '200_001_transform_customers_silver'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
