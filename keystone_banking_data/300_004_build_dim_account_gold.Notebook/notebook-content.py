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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer — `dim_account` (SCD Type 2)
# 
# **Notebook:** `300_004_build_dim_account_gold`  
# **Source:** `lh_silver_banking_data.accounts`  
# **Target:** `lh_gold_banking_data.dim_account`  
# **Layer:** Gold  
# 
# ---
# 
# ## What This Notebook Does
# 
# | Step | Operation |
# |---|---|
# | 1 | Load Silver accounts — deduplicated current snapshot |
# | 2 | Detect changed rows (SCD Type 2 tracked columns) |
# | 3 | Expire existing rows for changed accounts (`is_current = False`, `effective_to = today`) |
# | 4 | Insert new rows for changed + new accounts (`is_current = True`, `effective_from = today`) |
# | 5 | Write to `dim_account` via Delta MERGE |
# | 6 | Write audit log to `control.gold_audit_log` |
# | 7 | Validation summary |
# 
# ---
# 
# ## SCD Type 2 Design
# 
# | Column | Role |
# |---|---|
# | `dim_account_key` | Surrogate PK — SHA-256(`account_id` \| `effective_from`) |
# | `account_id` | Natural key from Silver (stable join key) |
# | `effective_from` | Date this row became active |
# | `effective_to` | Date this row was superseded (`9999-12-31` = current) |
# | `is_current` | Boolean convenience flag |
# 
# **Tracked attributes (trigger a new SCD row on change):**  
# `account_status`, `overdraft_limit`, `credit_card_limit`, `interest_rate`
# 
# ---


# MARKDOWN ********************

# ## 1. Configuration & Imports

# CELL ********************

import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DateType, DoubleType,
    BooleanType, IntegerType, TimestampType
)

# Capture batch start time
START_TIME = datetime.datetime.utcnow()

# Batch identity
GOLD_BATCH_ID  = START_TIME.strftime("%Y%m%dT%H%M%SZ")
SOURCE_TABLE   = "lh_silver_banking_data.dbo.accounts"
TARGET_TABLE   = "dim_account"
PIPELINE_NAME  = "300_004_build_dim_account_gold"

# SCD Type 2 sentinel date — open-ended current rows
SCD_END_DATE   = "9999-12-31"

# Columns whose change triggers a new SCD Type 2 row
SCD_TRACKED = ["account_status", "overdraft_limit", "credit_card_limit", "interest_rate"]

print(f"Gold batch  : {GOLD_BATCH_ID}")
print(f"Pipeline    : {PIPELINE_NAME}")
print(f"Source      : {SOURCE_TABLE}")
print(f"Target      : {TARGET_TABLE}")
print(f"SCD tracked : {SCD_TRACKED}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Audit Log 


# CELL ********************

spark.sql("""
CREATE SCHEMA IF NOT EXISTS control
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

print("✅ Gold audit log table is ready")

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

# ## Audit Log Update

# MARKDOWN ********************

# ## Load Silver Accounts
# 
# Silver `accounts` is a current-state snapshot — one row per `account_id` with `is_current = True`.  
# Filter to `is_current = True` to guard against any future Silver SCD expansion.

# CELL ********************

silver = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("is_current") == True)
)

silver_count = silver.count()
print(f"Silver rows loaded : {silver_count:,}")

# Sanity: confirm no duplicate account_ids in Silver current snapshot
dupes = silver.groupBy("account_id").count().filter(F.col("count") > 1).count()
print(f"Duplicate account_ids in Silver : {dupes}")
assert dupes == 0, "❌ Duplicate account_ids found in Silver — investigate before proceeding"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build Incoming Staging DataFrame
# 
# Select and rename columns for the Gold dimension.  
# SCD columns will be assigned during the merge logic below.

# CELL ********************

TODAY = F.current_date()

staging = (
    silver

    # Natural key
    .withColumn("account_id",              F.col("account_id").cast(StringType()))

    # SCD Type 2 tracked attributes ---
    .withColumn("account_status",          F.col("account_status").cast(StringType()))
    .withColumn("overdraft_limit",         F.col("overdraft_limit").cast(DoubleType()))
    .withColumn("credit_card_limit",       F.col("credit_card_limit").cast(DoubleType()))
    .withColumn("interest_rate",           F.col("interest_rate").cast(DoubleType()))

    # -Static / pass-through attributes
    .withColumn("account_type",            F.col("account_type").cast(StringType()))
    .withColumn("account_tier",            F.col("account_tier").cast(StringType()))
    .withColumn("tier_label",              F.col("tier_label").cast(StringType()))
    .withColumn("currency",                F.col("currency").cast(StringType()))
    .withColumn("opening_date",            F.col("opening_date").cast(DateType()))
    .withColumn("branch_code",             F.col("branch_code").cast(StringType()))
    .withColumn("opening_channel",         F.col("opening_channel").cast(StringType()))
    .withColumn("is_primary_account",      F.col("is_primary_account").cast(BooleanType()))
    .withColumn("has_overdraft",           F.col("has_overdraft").cast(BooleanType()))
    .withColumn("has_credit_card",         F.col("has_credit_card").cast(BooleanType()))
    .withColumn("is_foreign_currency",     F.col("is_foreign_currency").cast(BooleanType()))
    .withColumn("is_joint_account",        F.col("is_joint_account").cast(BooleanType()))
    .withColumn("is_business_account",     F.col("is_business_account").cast(BooleanType()))
    .withColumn("card_category",           F.col("card_category").cast(StringType()))
    .withColumn("kyc_verified",            F.col("kyc_verified").cast(BooleanType()))
    .withColumn("fica_verified",           F.col("fica_verified").cast(BooleanType()))
    .withColumn("account_age_days",        F.col("account_age_days").cast(IntegerType()))
    .withColumn("account_age_band",        F.col("account_age_band").cast(StringType()))
    .withColumn("onboarding_doc_score",    F.col("onboarding_doc_score").cast(IntegerType()))

    # Audit columns
    .withColumn("record_source",           F.lit("lh_silver_banking_data.accounts"))
    .withColumn("gold_batch_id",           F.lit(GOLD_BATCH_ID))
    .withColumn("gold_load_timestamp",     F.current_timestamp())

    .select(
        "account_id",
        # SCD tracked
        "account_status", "overdraft_limit", "credit_card_limit", "interest_rate",
        # Static
        "account_type", "account_tier", "tier_label", "currency",
        "opening_date", "branch_code", "opening_channel", "is_primary_account",
        "has_overdraft", "has_credit_card", "is_foreign_currency",
        "is_joint_account", "is_business_account", "card_category",
        "kyc_verified", "fica_verified",
        "account_age_days", "account_age_band", "onboarding_doc_score",
        # Audit
        "record_source", "gold_batch_id", "gold_load_timestamp"
    )
)

print(f"Staging columns : {len(staging.columns)}")
print(f"Staging rows    : {staging.count():,}")
staging.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## First Run — Create `dim_account` Table
# 
# On first run there is no target table. Write all incoming rows as `is_current = True`  
# with `effective_from = opening_date` (account's real start) and `effective_to = 9999-12-31`.

# CELL ********************

def build_surrogate_key(df):
    """
    Generate dim_account_key as SHA-256(account_id | effective_from).
    Must be called AFTER effective_from is assigned.
    """
    return df.withColumn(
        "dim_account_key",
        F.sha2(
            F.concat(
                F.col("account_id"),
                F.lit("|"),
                F.col("effective_from").cast(StringType())
            ),
            256
        )
    )


rows_inserted = 0
rows_expired  = 0

if not spark.catalog.tableExists(TARGET_TABLE):

    first_load = (
        staging
        # Use opening_date as the SCD effective start for initial load
        .withColumn("effective_from", F.col("opening_date"))
        .withColumn("effective_to",   F.lit(SCD_END_DATE).cast(DateType()))
        .withColumn("is_current",     F.lit(True))
    )

    first_load = build_surrogate_key(first_load)

    # Reorder so dim_account_key is the first column
    cols_ordered = ["dim_account_key", "account_id", "effective_from", "effective_to", "is_current"] + [
        c for c in first_load.columns
        if c not in ("dim_account_key", "account_id", "effective_from", "effective_to", "is_current")
    ]
    first_load = first_load.select(cols_ordered)

    (
        first_load.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )

    rows_inserted = first_load.count()
    print(f"✅ Created {TARGET_TABLE} (first run)")
    print(f"   Inserted : {rows_inserted:,}")
    print(f"   Expired  : 0")

else:
    print(f"Table {TARGET_TABLE} already exists — proceeding to SCD merge")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Incremental Run — SCD Type 2 Merge
# 
# **Logic:**
# 1. Join incoming `staging` against the current Gold rows (`is_current = True`)
# 2. Identify **changed** rows — where any tracked column differs
# 3. Identify **new** rows — `account_id` not in Gold at all
# 4. **Expire** old current rows for changed accounts (`is_current = False`, `effective_to = today - 1`)
# 5. **Insert** new rows for changed + new accounts (`effective_from = today`, `effective_to = 9999-12-31`, `is_current = True`)

# CELL ********************

if spark.catalog.tableExists(TARGET_TABLE) and rows_inserted == 0:
    # Only runs on incremental (not first-run) batches

    dim_current = (
        spark.table(TARGET_TABLE)
        .filter(F.col("is_current") == True)
        .select(
            "account_id",
            *[F.col(c).alias(f"existing_{c}") for c in SCD_TRACKED]
        )
    )

    # Join incoming staging to current Gold rows
    joined = staging.join(dim_current, on="account_id", how="left")

    # Build change detection condition
    # A row is changed when ANY tracked column differs from the existing value
    # Use eqNullSafe to handle NULLs correctly (NULL != NULL should NOT be flagged as a change)
    change_condition = F.lit(False)
    for col in SCD_TRACKED:
        change_condition = change_condition | (
            ~F.col(col).eqNullSafe(F.col(f"existing_{col}"))
            & F.col(f"existing_{col}").isNotNull()   # existing row found
        )

    new_condition = F.col(f"existing_{SCD_TRACKED[0]}").isNull()  # no match = new account

    changed_rows = joined.filter(change_condition)
    new_rows     = joined.filter(new_condition)

    changed_count = changed_rows.count()
    new_count     = new_rows.count()

    print(f"Changed accounts : {changed_count:,}")
    print(f"New accounts     : {new_count:,}")

    # Step 1: Expire old current rows for changed accounts
    if changed_count > 0:
        changed_ids = changed_rows.select("account_id")

        dim_table = DeltaTable.forName(spark, TARGET_TABLE)

        expire_df = (
            changed_ids
            .withColumn("is_current",   F.lit(False))
            .withColumn("effective_to", F.date_sub(F.current_date(), 1))
        )

        (
            dim_table.alias("t")
            .merge(
                expire_df.alias("s"),
                "t.account_id = s.account_id AND t.is_current = true"
            )
            .whenMatchedUpdate(set={
                "is_current":   "false",
                "effective_to": "s.effective_to"
            })
            .execute()
        )

        rows_expired = changed_count
        print(f"✅ Expired {rows_expired:,} rows")

    # Step 2: Insert new rows for changed + new accounts 
    to_insert = (
        changed_rows.drop(*[f"existing_{c}" for c in SCD_TRACKED])
        .union(
            new_rows.drop(*[f"existing_{c}" for c in SCD_TRACKED])
        )
        .withColumn("effective_from", F.current_date())
        .withColumn("effective_to",   F.lit(SCD_END_DATE).cast(DateType()))
        .withColumn("is_current",     F.lit(True))
    )

    to_insert = build_surrogate_key(to_insert)

    # Reorder columns to match target schema
    target_cols = spark.table(TARGET_TABLE).columns
    to_insert = to_insert.select(target_cols)

    rows_inserted = to_insert.count()

    if rows_inserted > 0:
        (
            to_insert.write
            .format("delta")
            .mode("append")
            .saveAsTable(TARGET_TABLE)
        )
        print(f"✅ Inserted {rows_inserted:,} new/updated rows")
    else:
        print("ℹ️  No changes detected — dim_account is up to date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write Gold Audit Log

# CELL ********************

END_TIME = datetime.datetime.utcnow()

audit_record = spark.createDataFrame(
    [(
        PIPELINE_NAME,
        GOLD_BATCH_ID,
        SOURCE_TABLE,
        TARGET_TABLE,
        silver_count,
        rows_inserted,
        rows_expired,
        START_TIME,
        END_TIME,
        "SUCCESS"
    )],
    """
    pipeline_name   STRING,
    batch_id        STRING,
    source_table    STRING,
    target_table    STRING,
    rows_read       BIGINT,
    rows_inserted   BIGINT,
    rows_expired    BIGINT,
    start_timestamp TIMESTAMP,
    end_timestamp   TIMESTAMP,
    status          STRING
    """
)

(
    audit_record.write
    .format("delta")
    .mode("append")
    .saveAsTable("control.gold_audit_log")
)

print(f"✅ Gold audit log updated")
print(f"   Duration : {(END_TIME - START_TIME).total_seconds():.1f}s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation Summary

# CELL ********************

dim = spark.table(TARGET_TABLE)
dim_current = dim.filter(F.col("is_current") == True)

print("=" * 65)
print("  GOLD BUILD SUMMARY — dim_account")
print("=" * 65)
print(f"""
  Batch ID        : {GOLD_BATCH_ID}
  Source          : {SOURCE_TABLE}
  Target          : {TARGET_TABLE}
  Silver rows in  : {silver_count:,}
  Rows inserted   : {rows_inserted:,}
  Rows expired    : {rows_expired:,}
  Total dim rows  : {dim.count():,}
  Current rows    : {dim_current.count():,}
  Historical rows : {dim.filter(F.col('is_current') == False).count():,}
""")

print("── Surrogate key uniqueness (current rows) ──")
key_dupes = dim_current.groupBy("dim_account_key").count().filter(F.col("count") > 1).count()
print(f"  Duplicate dim_account_keys : {key_dupes}  {'✅' if key_dupes == 0 else '❌ INVESTIGATE'}")

print()
print("── Account status distribution (current) ──")
dim_current.groupBy("account_status").count().orderBy("count", ascending=False).show()

print("── Tier distribution (current) ──")
dim_current.groupBy("account_tier", "tier_label").count().orderBy("account_tier").show()

print("── SCD history depth ──")
dim.groupBy("account_id") \
  .agg(F.count("*").alias("versions"))\
  .groupBy("versions")\
  .agg(F.count("*").alias("account_count")) \
  .orderBy("versions")\
  .show()

print("── Sample current rows ──")
display(
    dim_current.select(
        "dim_account_key", "account_id", "account_status",
        "account_tier", "overdraft_limit", "credit_card_limit",
        "interest_rate", "effective_from", "effective_to", "is_current"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Delete transactions row from both control tables

spark.sql("""
    DELETE FROM lh_gold_banking_data.control.gold_audit_log
    WHERE pipeline_name = '300_004_build_dim_account_gold'
""")

print("✅ Deleted customers rows from both control tables")



spark.sql("""
    SELECT pipeline_name, batch_id, status FROM lh_gold_banking_data.control.gold_audit_log
    WHERE pipeline_name = '300_004_build_dim_account_gold'
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

spark.table("lh_silver_banking_data.dbo.accounts") \
    .filter(F.col("has_overdraft") == True) \
    .select("account_id", "has_overdraft", "overdraft_limit") \
    .show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("lh_gold_banking_data.dbo.dim_account") \
    .filter(F.col("has_overdraft") == True) \
    .select("account_id", "has_overdraft", "overdraft_limit") \
    .show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
