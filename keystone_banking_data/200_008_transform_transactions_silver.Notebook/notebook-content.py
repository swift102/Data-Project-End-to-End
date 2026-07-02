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
# META         },
# META         {
# META           "id": "e1b0fd50-8d63-4667-998b-3fd590fa7ff9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver Transform 
# 
# **Notebook:** `200_008_transform_transactions_silver`  
# **Source:** `lh_bronze_banking_data.dbo.bronze_transactions` (4,991,101 rows)  
# **Target:** `lh_silver_banking_data.transactions`  
# **Layer:** Silver  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 |  Load Bronze + composite-key dedup │ Natural key is (transaction_id, transaction_date) — not transaction_id alone │ transaction_id is reused for recurring debit orders across months |
# | 2 | Extract nested struct fields into flat columns, then drop structs |
# | 3 | Enforce explicit schema |
# | 4 | Null classification — required vs conditionally null vs empty |
# | 5 | PII masking — hash beneficiary_name, partial mask receiving_account |
# | 6 | Derived columns — `transaction_sk `, `channel`, `currency`, `is_completed`, `is_debit`, `is_credit`, `is_recurring`, `is_loan_payment`, `is_debit_order` |
# | 7 | Write `transactions` to Silver + validation summary |
# 
# ---
# 
# ## Key Findings from Bronze Profiling
# 
# | Finding | Detail |
# |---|---|
# | Natural key | `transaction_id` alone: 4,442,333 distinct / 4,991,101 rows — NOT unique. `(transaction_id, transaction_date)`: 4,991,101 distinct — UNIQUE |
# | Recurring IDs | 56,162 `transaction_id`s are recurring debit order mandates reusing the same ID monthly — source system design, not a DQ issue |
# | `transaction_time` | Date portion = extraction date (2026-06-01), not tx date — corrupted, dropped in Silver |
# | `transaction_timestamp` | Correct combination of date + time — used as source of truth instead |
# | Column redundancy | 11 flat columns byte-for-byte identical to struct counterparts (confirmed via sampling) — flat versions dropped, struct fields extracted with clear names, structs dropped |
# | `ewallet_number` | 4,991,101 nulls (100%) — dropped |
# | `currency` nulls | 3,115,702 nulls — filled to `'ZAR'` |
# | Channel casing | `'Online'` = `online_banking`, `'Mobile'` = `mobile_banking_app` |
# | Status | All statuses kept in Silver (Gold filters to completed) |
# | Transaction subtypes | `debit_order` 1,901,211 · `loan_payment` 607,065 · `spending` 2,482,825 |


# MARKDOWN ********************

# ## Configuration & Imports
# 


# CELL ********************

import datetime
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StringType, DateType, DoubleType,
    BooleanType, IntegerType, TimestampType, LongType
)

config = json.loads(
    notebookutils.notebook.run("000_Config", 90, {"useRootDefaultLakehouse": True})
)
MASK_SALT = config["MASK_SALT"]

SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
PIPELINE_NAME   = "200_008_transform_transactions_silver"
SOURCE_TABLE    = "lh_bronze_banking_data_modern_data.dbo.bronze_transactions"
TARGET_TABLE    = "transactions"
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

print(f"Last watermark : {last_watermark}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze & Composite-Key Dedup


# CELL ********************

# Natural key: (transaction_id, transaction_date)
# Dedup order: latest _ingest_timestamp wins.
# On the first run all rows are unique on the composite key.
# On incremental runs, the same (id, date) may re-arrive if
# the source file is re-ingested; keep the latest copy.

bronze_full = spark.table(SOURCE_TABLE)

if last_watermark is None:
    bronze = bronze_full
    print("First run — full load")
else:
    bronze = bronze_full.filter(
        F.col(WATERMARK_COL) > F.lit(last_watermark)
    )
    print(f"Incremental — records after {last_watermark}")

print(f"Bronze rows loaded : {bronze.count():,}")

w = Window.partitionBy("transaction_id", "transaction_date").orderBy(
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

new_watermark = (
    bronze.agg(F.max(WATERMARK_COL)).collect()[0][0]
    if bronze.count() > 0
    else last_watermark
)
print(f"New watermark      : {new_watermark}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extract Struct Fields

# CELL ********************

# Flatten struct columns into clearly named flat columns before
# dropping the structs. Naming convention:
#   debit order fields  → do_{field}
#   loan fields         → loan_{field}
#   channel fields      → ch_{field}
#
# Flat redundant columns dropped here alongside their structs:
#   debit_order_id, debit_order_type, frequency,
#   is_immediate_payment, immediate_payment,
#   loan_id, loan_type, payment_type,
#   installment_number, is_recovery_attempt

REDUNDANT_FLAT_COLS = [
    "debit_order_id",
    "debit_order_type",
    "frequency",
    "is_immediate_payment",
    "immediate_payment",
    "loan_id",
    "loan_type",
    "payment_type",
    "installment_number",
    "is_recovery_attempt",
]

extracted = (
    deduped

    # drop original flat duplicates FIRST — prevents the loan_id/loan_type
    # name collision with the struct-extracted columns below
    .drop(*REDUNDANT_FLAT_COLS)

    # debit_order_metadata
    .withColumn("do_debit_order_id", F.col("debit_order_metadata.debit_order_id"))
    .withColumn("do_debit_order_type", F.col("debit_order_metadata.debit_order_type"))
    .withColumn("do_frequency", F.col("debit_order_metadata.frequency"))
    .withColumn("do_beneficiary_name", F.col("debit_order_metadata.beneficiary_name"))
    .withColumn("do_is_immediate_payment", F.col("debit_order_metadata.is_immediate_payment"))

    # loan_payment_metadata
    .withColumn("loan_id", F.col("loan_payment_metadata.loan_id"))
    .withColumn("loan_type", F.col("loan_payment_metadata.loan_type"))
    .withColumn("loan_payment_type", F.col("loan_payment_metadata.payment_type"))
    .withColumn("loan_installment_number", F.col("loan_payment_metadata.installment_number"))
    .withColumn("loan_is_recovery_attempt", F.col("loan_payment_metadata.is_recovery_attempt"))
    .withColumn("loan_is_immediate_payment", F.col("loan_payment_metadata.immediate_payment"))

    # channel_metadata (selective)
    # Only fields with downstream analytical value extracted.
    # gps_coordinates, session_duration_seconds, retry_count etc.
    # remain in the struct and are dropped with it.
    .withColumn("ch_payment_network", F.col("channel_metadata.payment_network"))
    .withColumn("ch_atm_id", F.col("channel_metadata.atm_id"))
    .withColumn("ch_branch_code", F.col("channel_metadata.branch_code"))
    .withColumn("ch_network_type", F.col("channel_metadata.network_type"))

    # only structs left to drop — flat redundants already gone
    .drop(
        "channel_metadata",
        "debit_order_metadata",
        "loan_payment_metadata",
        "error_metadata",
    )

    .drop(
        "transaction_time",
        "ewallet_number",
    )
)

print(f"✅ Struct fields extracted")
print(f"Columns after extraction : {len(extracted.columns)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Enforce Explicit Schema


# CELL ********************

typed = (
    extracted

    # Identity
    .withColumn("transaction_id",
        F.trim(F.col("transaction_id")).cast(StringType()))
    .withColumn("account_id",
        F.col("account_id").cast(StringType()))

    # Authoritative timestamp
    # transaction_timestamp = correct date + correct time.
    # transaction_time was DROPPED (date portion = extraction date bug).
    # transaction_date kept separately for date-level partitioning.
    .withColumn("transaction_timestamp",
        F.to_timestamp(F.col("transaction_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("transaction_date",
        F.col("transaction_date").cast(DateType()))

    # Amounts
    .withColumn("amount",
        F.col("amount").cast(DoubleType()))
    .withColumn("transaction_cost",
        F.col("transaction_cost").cast(DoubleType()))
    .withColumn("authorization_time_ms",
        F.col("authorization_time_ms").cast(LongType()))

    # Classification
    .withColumn("category",
        F.trim(F.col("category")).cast(StringType()))
    .withColumn("debit_credit",
        F.trim(F.lower(F.col("debit_credit"))).cast(StringType()))
    .withColumn("status",
        F.trim(F.lower(F.col("status"))).cast(StringType()))
    .withColumn("description",
        F.trim(F.col("description")).cast(StringType()))

    # Channel — normalise inconsistent casing
    # 'Online' (3,901 rows) → 'online_banking'
    # 'Mobile' (1,750 rows) → 'mobile_banking_app'
    # All others → lowercase + trim
    .withColumn("channel",
        F.when(F.trim(F.col("channel")) == "Online",  F.lit("online_banking"))
         .when(F.trim(F.col("channel")) == "Mobile",  F.lit("mobile_banking_app"))
         .otherwise(F.trim(F.lower(F.col("channel"))))
         .cast(StringType()))

    # Currency — null means ZAR 
    .withColumn("currency",
        F.coalesce(
            F.trim(F.upper(F.col("currency"))),
            F.lit("ZAR")
        ).cast(StringType()))

    # Debit order extracted fields
    .withColumn("do_debit_order_id",
        F.trim(F.col("do_debit_order_id")).cast(StringType()))
    .withColumn("do_debit_order_type",
        F.trim(F.col("do_debit_order_type")).cast(StringType()))
    .withColumn("do_frequency",
        F.trim(F.col("do_frequency")).cast(StringType()))
    .withColumn("do_beneficiary_name",
        F.trim(F.col("do_beneficiary_name")).cast(StringType()))
    .withColumn("do_is_immediate_payment",
        F.col("do_is_immediate_payment").cast(BooleanType()))

    # Loan extracted fields 
    .withColumn("loan_id",
        F.trim(F.col("loan_id")).cast(StringType()))
    .withColumn("loan_type",
        F.trim(F.col("loan_type")).cast(StringType()))
    .withColumn("loan_payment_type",
        F.trim(F.col("loan_payment_type")).cast(StringType()))
    .withColumn("loan_installment_number",
        F.col("loan_installment_number").cast(IntegerType()))
    .withColumn("loan_is_recovery_attempt",
        F.col("loan_is_recovery_attempt").cast(BooleanType()))
    .withColumn("loan_is_immediate_payment",
        F.col("loan_is_immediate_payment").cast(BooleanType()))

    # Channel extracted fields
    .withColumn("ch_payment_network",
        F.trim(F.col("ch_payment_network")).cast(StringType()))
    .withColumn("ch_atm_id",
        F.trim(F.col("ch_atm_id")).cast(StringType()))
    .withColumn("ch_branch_code",
        F.trim(F.col("ch_branch_code")).cast(StringType()))
    .withColumn("ch_network_type",
        F.trim(F.col("ch_network_type")).cast(StringType()))

    # PII / reference fields 
    .withColumn("beneficiary_name",
        F.trim(F.col("beneficiary_name")).cast(StringType()))
    .withColumn("merchant_name",
        F.trim(F.col("merchant_name")).cast(StringType()))
    .withColumn("receiving_account",
        F.trim(F.col("receiving_account")).cast(StringType()))

    # Error & status fields
    .withColumn("has_error",
        F.col("has_error").cast(BooleanType()))
    .withColumn("has_data_error",
        F.col("has_data_error").cast(BooleanType()))
    .withColumn("failure_reason",
        F.trim(F.col("failure_reason")).cast(StringType()))
    .withColumn("error_types",
        F.trim(F.col("error_types")).cast(StringType()))
    .withColumn("data_error_types",
        F.trim(F.col("data_error_types")).cast(StringType()))
    .withColumn("third_party_timeout",
        F.trim(F.col("third_party_timeout")).cast(StringType()))

    # Payment network references 
    .withColumn("rrn",  F.trim(F.col("rrn")).cast(StringType()))
    .withColumn("stan", F.trim(F.col("stan")).cast(StringType()))

    # Metadata columns
    .withColumn("source_system",
        F.trim(F.col("source_system")).cast(StringType()))
    .withColumn("record_last_updated_at",
        F.to_timestamp(F.col("record_last_updated_at")))
)

print("✅ Schema enforced")
print(f"Columns after typing : {len(typed.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Null Classification

# CELL ********************

REQUIRED = [
    "transaction_id", "account_id", "transaction_timestamp",
    "transaction_date", "amount", "debit_credit", "status", "channel"
]

print("=" * 55)
print("REQUIRED FIELD NULL CHECK")
print("=" * 55)
all_pass = True
for col in REQUIRED:
    n = typed.filter(F.col(col).isNull()).count()
    flag = "✅" if n == 0 else "❌ FAIL"
    if n > 0:
        all_pass = False
    print(f"  {flag}  {col}: {n:,} nulls")

print()
print("EXPECTED NULLS (by design — transaction subtype)")
expected_nulls = {
    "do_debit_order_id"       : "null for non-debit-order transactions",
    "do_debit_order_type"     : "null for non-debit-order transactions",
    "do_frequency"            : "null for non-debit-order transactions",
    "loan_id"                 : "null for non-loan transactions",
    "loan_type"               : "null for non-loan transactions",
    "loan_installment_number" : "null for non-loan transactions",
    "beneficiary_name"        : "null for non-transfer/non-debit-order transactions",
    "merchant_name"           : "null for non-card transactions",
    "failure_reason"          : "null for non-failed transactions",
    "ch_atm_id"               : "null for non-ATM transactions",
    "authorization_time_ms"   : "null for debit orders and loan payments",
    "rrn"                     : "null for debit orders and loan payments",
    "stan"                    : "null for debit orders and loan payments",
    "record_last_updated_at"  : "null for non-CDC transactions",
    "transaction_cost"        : "null where fee not applicable",
}
for col, reason in expected_nulls.items():
    if col in typed.columns:
        n = typed.filter(F.col(col).isNull()).count()
        print(f"  ✅  {col}: {n:,} nulls — {reason}")

print(f"\nAll required fields pass : {all_pass}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PII Masking


# CELL ********************

# Masking strategy:
# ┌────────────────────┬──────────────────┬──────────────────────────────────┐
# │ Column             │ Technique        │ Reason                           │
# ├────────────────────┼──────────────────┼──────────────────────────────────┤
# │ beneficiary_name   │ Full hash        │ Person's name on transfers       │
# │ do_beneficiary_name│ Full hash        │ Person's name on debit orders    │
# │ receiving_account  │ Partial mask     │ Account number — last 4 visible  │
# └────────────────────┴──────────────────┴──────────────────────────────────┘
# merchant_name: NOT masked — business trading name, not personal data
# description  : NOT masked — free text, but does not reliably contain PII;
#                flag as sensitive for downstream consumers

masked = (
    typed

    # Full hash: beneficiary_name
    .withColumn("beneficiary_name",
        F.when(
            F.col("beneficiary_name").isNotNull(),
            F.sha2(F.concat(F.col("beneficiary_name"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Full hash: do_beneficiary_name (debit order payee — also a person's name)
    .withColumn("do_beneficiary_name",
        F.when(
            F.col("do_beneficiary_name").isNotNull(),
            F.sha2(F.concat(F.col("do_beneficiary_name"), F.lit(MASK_SALT)), 256)
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # Partial mask: receiving_account — show last 4 digits
    .withColumn("receiving_account",
        F.when(
            F.col("receiving_account").isNotNull(),
            F.concat(F.lit("****"), F.substring(F.col("receiving_account"), -4, 4))
        ).otherwise(F.lit(None).cast(StringType()))
    )
)

print("✅ PII masking applied")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Derived Columns

# CELL ********************

enriched = (
    masked

    # Surrogate key
    # Composite natural key: (transaction_id, transaction_date)
    # transaction_id alone is NOT unique (recurring debit orders reuse it).
    .withColumn("transaction_sk",
        F.xxhash64(F.col("transaction_id"), F.col("transaction_date"))
    )

    # Status flags
    .withColumn("is_completed",
        F.col("status") == "completed")
    .withColumn("is_failed",
        F.col("status") == "failed")
    .withColumn("is_reversed",
        F.col("status") == "reversed")

    # Direction flags
    .withColumn("is_debit",
        F.col("debit_credit") == "debit")
    .withColumn("is_credit",
        F.col("debit_credit") == "credit")

    # Transaction subtype flags
    # These are mutually exclusive by source design.
    .withColumn("is_debit_order",
        F.col("category") == "debit_order")
    .withColumn("is_loan_payment",
        F.col("category") == "loan_payment")
    .withColumn("is_scheduled",
        F.col("category") == "scheduled_payment")

    # Recurring flag
    # A transaction is recurring if it is a debit order or scheduled payment.
    # These are the categories that reuse transaction_id across months.
    .withColumn("is_recurring",
        F.col("category").isin("debit_order", "scheduled_payment"))

    # Salary proxy flag
    # Salary-like debit orders: credit direction + recurring + commonly
    # used debit order types. This is a proxy flag for salary detection.
    .withColumn("is_salary_candidate",
        F.col("is_credit") &
        F.col("is_debit_order") &
        F.col("do_debit_order_type").isin(
            "Salary", "Payroll", "Income", "Wage"
        )
    )

    # Time components
    # Extracted from authoritative transaction_timestamp for
    # aggregation convenience in gold.
    .withColumn("transaction_year",
        F.year(F.col("transaction_timestamp")).cast(IntegerType()))
    .withColumn("transaction_month",
        F.month(F.col("transaction_timestamp")).cast(IntegerType()))
    .withColumn("transaction_day_of_week",
        F.dayofweek(F.col("transaction_timestamp")).cast(IntegerType()))

    # Audit columns
    .withColumn("record_source",      F.lit("bronze_transactions"))
    .withColumn("created_timestamp",  F.current_timestamp())
    .withColumn("updated_timestamp",  F.current_timestamp())
    .withColumn("silver_batch_id",    F.lit(SILVER_BATCH_ID))
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

# ## Final Column Selection


# CELL ********************

SILVER_COLS = [

    # Keys
    "transaction_sk",           # surrogate key: xxhash64(transaction_id, transaction_date)
    "transaction_id",           # natural key component 1 (NOT unique alone)
    "account_id",               # FK → silver accounts

    # Timestamps 
    "transaction_timestamp",    # authoritative — use for all temporal metrics
    "transaction_date",         # date component — use for date-level partitioning
    "transaction_year",
    "transaction_month",
    "transaction_day_of_week",

    # Core transaction fields 
    "amount",
    "currency",
    "debit_credit",
    "status",
    "category",
    "channel",
    "description",
    "transaction_cost",
    "authorization_time_ms",

    # Status flags
    "is_completed",
    "is_failed",
    "is_reversed",

    # Direction flags 
    "is_debit",
    "is_credit",

    # Subtype flags
    "is_debit_order",
    "is_loan_payment",
    "is_scheduled",
    "is_recurring",
    "is_salary_candidate",


    # Debit order fields (null for non-debit-order rows)
    "do_debit_order_id",
    "do_debit_order_type",
    "do_frequency",
    "do_beneficiary_name",      # hashed
    "do_is_immediate_payment",

    # Loan fields (null for non-loan rows)
    "loan_id",
    "loan_type",
    "loan_payment_type",
    "loan_installment_number",
    "loan_is_recovery_attempt",
    "loan_is_immediate_payment",

    # Channel metadata (null for non-card rows)
    "ch_payment_network",
    "ch_atm_id",
    "ch_branch_code",
    "ch_network_type",

    # Parties
    "merchant_name",            # not masked — business name, not personal data
    "beneficiary_name",         # hashed
    "receiving_account",        # partial mask ****NNNN

    # Error fields
    "has_error",
    "has_data_error",
    "error_types",
    "data_error_types",
    "failure_reason",
    "third_party_timeout",

    # Payment network references 
    "rrn",
    "stan",

    # Other
    "source_system",
    "record_last_updated_at",

    # Audit
    "record_source",
    "created_timestamp",
    "updated_timestamp",
    "silver_batch_id",
    "silver_load_timestamp",
    "_source_file",
    "_ingest_timestamp",
    "_batch_id",
    "_commit_sha",
    "year",
    "month",
]

silver = enriched.select(SILVER_COLS)

print(f"✅ Column selection applied : {len(silver.columns)} columns")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initial Deposits Union
# 
# `bronze.bronze_initial_deposits` contains account opening deposits partitioned across
# multiple month/year files, absent from `bronze_transactions` entirely. These records are
# unioned into the transactions DataFrame here, before `merge_silver()`, so that
# `silver_transactions` and downstream `fact_transaction` reflect correct opening balances
# and per-account net flow from day one.
# 
# **Source table:** `bronze.bronze_initial_deposits` (backfilled via `100_002`)
# **Coverage:** all months/years present in `initial_deposits/` at time of backfill run
# **Flag:** `is_initial_deposit = True` — retained in Silver for filtering at Gold/reporting layer
# **No `transaction_id` in source** — field carried as `NULL`; Silver does not synthesise a key for Bronze-faithful records

# CELL ********************

# Read initial deposits from Bronze
df_initial = (
    spark.table("lh_bronze_banking_data_modern_data.dbo.bronze_initial_deposits")
    .select(
        F.col("account_id"),
        F.lit(None).cast("string").alias("transaction_id"),
        F.xxhash64(F.col("account_id"), F.col("transaction_date")).alias("transaction_sk"),
        F.col("transaction_date").cast(TimestampType()).alias("transaction_timestamp"),
        F.col("transaction_date").cast(DateType()).alias("transaction_date"),
        F.year(F.col("transaction_date")).cast(StringType()).alias("transaction_year"),
        F.month(F.col("transaction_date")).cast(StringType()).alias("transaction_month"),
        F.col("amount"),
        F.lit("ZAR").alias("currency"),
        F.lit("credit").alias("debit_credit"),
        F.lit("completed").alias("status"),
        F.col("transaction_type").alias("category"),
        F.col("channel"),
        F.lit("Initial account deposit").alias("description"),
        F.lit(True).alias("is_completed"),
        F.lit(False).alias("is_failed"),
        F.lit(False).alias("is_reversed"),
        F.lit(False).alias("is_debit"),
        F.lit(True).alias("is_credit"),
        F.lit(False).alias("is_debit_order"),
        F.lit(False).alias("is_loan_payment"),
        F.lit(False).alias("is_scheduled"),
        F.lit(False).alias("is_recurring"),
        F.lit(False).alias("is_salary_candidate"),
        F.lit(True).alias("is_initial_deposit"),
        F.lit("initial_deposits").alias("record_source"),
        F.current_timestamp().alias("created_timestamp"),
        F.current_timestamp().alias("updated_timestamp"),
        F.lit(SILVER_BATCH_ID).alias("silver_batch_id"),
        F.current_timestamp().alias("silver_load_timestamp"),
        F.col("_ingest_timestamp"),
        F.col("_batch_id"),
        F.regexp_extract(F.col("_source_file"), r"/(\d{4})/", 1).alias("year"),
        F.regexp_extract(F.col("_source_file"), r"/\d{4}/(\d{2})/", 1).alias("month"),
    )
)

# Tag silver df and union
silver = silver.withColumn("is_initial_deposit", F.lit(False))
silver = silver.unionByName(df_initial, allowMissingColumns=True)

print(f"Silver rows after initial deposits union : {silver.count():,}")

silver = silver.withColumn("is_initial_deposit", F.lit(False))
silver = silver.unionByName(df_initial, allowMissingColumns=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    ALTER TABLE transactions
    ADD COLUMN is_initial_deposit BOOLEAN
""")

spark.sql("""
    UPDATE transactions
    SET is_initial_deposit = FALSE
    WHERE is_initial_deposit IS NULL
""")

print("✅ is_initial_deposit column added and backfilled")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Silver — Delta Merge

# CELL ********************

# Merge key: transaction_sk (deterministic surrogate on composite natural key)
# created_timestamp is never overwritten on update — it records first-seen date.

from delta.tables import DeltaTable

def merge_silver(df, table_name, merge_key):

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
        print(f"   Inserts : {inserts:,}")
        print(f"   Updates : {updates:,}")
        return inserts, updates   # ← explicit return required on first-load path

    # Incremental merge
    existing_keys = spark.table(table_name).select(merge_key)

    inserts = df.join(existing_keys, merge_key, "left_anti").count()
    updates = df.count() - inserts

    update_set = {
        c: f"s.{c}"
        for c in df.columns
        if c != "created_timestamp"   # preserve original creation date
    }
    update_set["updated_timestamp"] = "current_timestamp()"

    (
        DeltaTable.forName(spark, table_name)
        .alias("t")
        .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged {table_name}")
    print(f"   Inserts : {inserts:,}")
    print(f"   Updates : {updates:,}")
    return inserts, updates   # ← return also required on merge path


rows_inserted, rows_updated = merge_silver(silver, TARGET_TABLE, "transaction_sk")
rows_written = silver.count()


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
        "rows_processed"      : rows_written,
        "rows_inserted"       : rows_inserted,
        "rows_updated"        : rows_updated,
        "status"              : "SUCCESS",
        "processed_timestamp" : datetime.datetime.utcnow(),
    }])
    .write.format("delta").mode("append")
    .saveAsTable("control.batch_watermark")
)

print(f"✅ Watermark updated to {new_watermark}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Audit Log

# CELL ********************

(
    spark.createDataFrame([{
        "pipeline_name"   : PIPELINE_NAME,
        "batch_id"        : SILVER_BATCH_ID,
        "source_table"    : SOURCE_TABLE,
        "rows_processed"  : rows_written,
        "rows_inserted"   : rows_inserted,
        "rows_updated"    : rows_updated,
        "start_timestamp" : datetime.datetime.utcnow(),
        "end_timestamp"   : datetime.datetime.utcnow(),
        "status"          : "SUCCESS",
    }])
    .write.format("delta").mode("append")
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

txn = spark.table(TARGET_TABLE)

print("=" * 65)
print("  SILVER TRANSFORM SUMMARY — transactions")
print("=" * 65)
print(f"""
  Batch ID     : {SILVER_BATCH_ID}
  Source       : {SOURCE_TABLE}
  Target       : {TARGET_TABLE}

  DEDUPLICATION
  Bronze rows loaded  : {bronze.count():,}
  Silver rows written : {rows_written:,}
  Duplicates removed  : {bronze.count() - rows_written:,}

  MERGE STATISTICS
  Rows processed : {rows_written:,}
  Rows inserted  : {rows_inserted:,}
  Rows updated   : {rows_updated:,}

  WATERMARK
  Previous : {last_watermark}
  New      : {new_watermark}
""")

print("Status distribution")
txn.groupBy("status").count().orderBy("count", ascending=False).show()

print("Category distribution")
txn.groupBy("category").count().orderBy("count", ascending=False).show()

print("Channel distribution")
txn.groupBy("channel").count().orderBy("count", ascending=False).show()

print("Currency distribution")
txn.groupBy("currency").count().orderBy("count", ascending=False).show()

print("Transaction subtype flags")
print(f"  is_debit_order      : {txn.filter(F.col('is_debit_order')).count():,}")
print(f"  is_loan_payment     : {txn.filter(F.col('is_loan_payment')).count():,}")
print(f"  is_recurring        : {txn.filter(F.col('is_recurring')).count():,}")
print(f"  is_salary_candidate : {txn.filter(F.col('is_salary_candidate')).count():,}")
print(f"  is_completed        : {txn.filter(F.col('is_completed')).count():,}")
print(f"  is_failed           : {txn.filter(F.col('is_failed')).count():,}")
print(f"  is_reversed         : {txn.filter(F.col('is_reversed')).count():,}")

print("=" * 65)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete transactions row from both control tables

spark.sql("""
    DELETE FROM lh_silver_banking_data.control.batch_watermark
    WHERE pipeline_name = '200_008_transform_transactions_silver'
""")

spark.sql("""
    DELETE FROM lh_silver_banking_data.control.silver_audit_log
    WHERE pipeline_name = '200_008_transform_transactions_silver'
""")

print("✅ Deleted customers rows from both control tables")

# Verify
spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_silver_banking_data.control.batch_watermark
    WHERE pipeline_name = '200_008_transform_transactions_silver'
""").show(truncate=False)

spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_silver_banking_data.control.silver_audit_log
    WHERE pipeline_name = '200_008_transform_transactions_silver'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
