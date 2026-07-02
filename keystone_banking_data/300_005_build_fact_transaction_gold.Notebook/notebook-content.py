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

# # Gold Layer — `fact_transaction`
# 
# **Notebook:** `300_005_build_fact_transaction_gold`  
# **Source:** `lh_silver_banking_data.transactions`  
# **Target:** `lh_gold_banking_data.fact_transaction`  
# **Layer:** Gold  
# 
# ---
# 
# ## Grain
# 
# One row per completed transaction — `transaction_sk` is the fact PK.  
# `transaction_id` alone is NOT unique (recurring debit orders reuse it monthly).  
# Natural key is `(transaction_id, transaction_date)`. Silver's `transaction_sk = xxhash64(transaction_id, transaction_date)`.  
# Initial deposit rows have `transaction_id = NULL` — their `transaction_sk` is `xxhash64(account_id, transaction_date)`.
# 
# ---
# 
# ## Foreign Keys
# 
# | FK Column | Resolves To | Join Logic |
# |---|---|---|
# | `date_key` | `dim_date` | `CAST(DATE_FORMAT(transaction_date, 'yyyyMMdd') AS INT)` |
# | `dim_account_key` | `dim_account` | Point-in-time: `account_id` + `transaction_date BETWEEN effective_from AND effective_to` |
# | `primary_customer_sk` | `dim_customer` | `bridge_customer_account` where `relationship_type = 'primary'` → `customer_sk` |
# | `product_code` | `dim_product` | From `dim_account.product_code` — joined via `dim_account_key` resolution |
# 
# ---
# 
# ## Measures
# 
# | Measure | Source | Notes |
# |---|---|---|
# | `transaction_amount` | `amount` | Signed: negative for debits |
# | `transaction_amount_abs` | `ABS(amount)` | Always positive — for aggregation |
# | `fee_amount` | `transaction_cost` | Null where no fee |
# | `authorization_time_ms` | `authorization_time_ms` | Performance metric |
# 
# ---
# 
# ## Steps
# 
# | Step | Operation |
# |---|---|
# | 1 | Load Silver transactions — filter to `is_completed = true` |
# | 2 | Derive `date_key` |
# | 3 | Resolve `dim_account_key` via point-in-time join to `dim_account` |
# | 4 | Resolve `product_code` from `dim_account` |
# | 5 | Resolve `primary_customer_sk` from `bridge_customer_account` |
# | 6 | Assemble final fact columns + FK coverage check |
# | 7 | Write to `fact_transaction` via Delta MERGE on `transaction_sk` |
# | 8 | Write audit log to `control.gold_audit_log` |
# | 9 | Validation summary |
# 
# ---


# MARKDOWN ********************

# ## Configuration & Imports

# CELL ********************

import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DateType, DoubleType,
    BooleanType, IntegerType, LongType, TimestampType
)

START_TIME = datetime.datetime.utcnow()

GOLD_BATCH_ID  = START_TIME.strftime("%Y%m%dT%H%M%SZ")
SOURCE_TABLE   = "lh_silver_banking_data.dbo.transactions"
TARGET_TABLE   = "fact_transaction"
PIPELINE_NAME  = "300_005_build_fact_transaction_gold"

# Dim tables
# Dim tables
DIM_ACCOUNT    = "dim_account"
DIM_CUSTOMER   = "dim_customer"
BRIDGE         = "lh_silver_banking_data.dbo.bridge_customer_account"
SILVER_ACCOUNTS = "lh_silver_banking_data.dbo.accounts"

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

# ## Add Audit Log 

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

print("✅ Gold audit log ready")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Silver Transactions
# 
# Gold only cares about completed transactions.  
# Failed and reversed rows stay in Silver for operational reporting but have no business
# value in the star schema fact table.

# CELL ********************

silver = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("is_completed") == True)
)

silver_count = silver.count()
print(f"Silver completed transactions : {silver_count:,}")

# Status breakdown for sanity
spark.table(SOURCE_TABLE).groupBy("status").count().orderBy("count", ascending=False).show()
print(f"Rows excluded (non-completed) : {spark.table(SOURCE_TABLE).count() - silver_count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Derive `date_key`
# 
# `date_key` is an integer in `YYYYMMDD` format — FK to `dim_date.date_key`.

# CELL ********************

staged = silver.withColumn(
    "date_key",
    F.date_format(F.col("transaction_date"), "yyyyMMdd").cast(IntegerType())
)

print("Sample date_keys:")
staged.select("transaction_date", "date_key").distinct().orderBy("transaction_date").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Resolve `dim_account_key` — Point-in-Time Join
# 
# Each transaction must join to the version of `dim_account` that was active at the time  
# of the transaction — i.e. the row where `transaction_date BETWEEN effective_from AND effective_to`.
# 
# **Generator artifact:** `opening_date` in Silver is sometimes *after* early transactions  
# for the same account. `dim_account` was built with `effective_from = opening_date`,  
# which means those transactions fall outside the SCD window and produce NULL joins.  
# 
# **Fix:** compute `earliest_tx_date` per account from Silver transactions, then  
# patch `dim_account.effective_from` to `LEAST(effective_from, earliest_tx_date)` inline  
# before the join. This is a read-time correction — the Delta table is not modified.

# CELL ********************

# Compute earliest completed transaction date per account from Silver
# Used to patch dim_account effective_from for the generator artifact
# where opening_date > earliest transaction date.
earliest_tx = (
    spark.table(SOURCE_TABLE)
    .filter(F.col("is_completed") == True)
    .groupBy("account_id")
    .agg(F.min("transaction_date").alias("earliest_tx_date"))
)

dim_acc = (
    spark.table(DIM_ACCOUNT)
    .select(
        "dim_account_key",
        "account_id",
        "effective_from",
        "effective_to",
        "account_type",
        "account_tier",
        "branch_code",
        "is_business_account",
        "is_joint_account",
    )
    # Patch effective_from: use LEAST(effective_from, earliest_tx_date)
    # so transactions predating the recorded opening_date still resolve.
    .join(earliest_tx, on="account_id", how="left")
    .withColumn(
        "effective_from_patched",
        F.least(F.col("effective_from"), F.col("earliest_tx_date"))
    )
    .drop("effective_from", "earliest_tx_date")
    .withColumnRenamed("effective_from_patched", "effective_from")
)

# Point-in-time join using patched effective_from
joined_acc = (
    staged.alias("t")
    .join(
        dim_acc.alias("a"),
        on=(
            (F.col("t.account_id") == F.col("a.account_id")) &
            (F.col("t.transaction_date") >= F.col("a.effective_from")) &
            (F.col("t.transaction_date") <= F.col("a.effective_to"))
        ),
        how="left"
    )
    .drop(F.col("a.account_id"))
)

# FK coverage check
unmatched_acc = joined_acc.filter(F.col("dim_account_key").isNull()).count()
print(f"Transactions with no dim_account match : {unmatched_acc:,}")
if unmatched_acc > 0:
    print("Sample unmatched:")
    joined_acc.filter(F.col("dim_account_key").isNull()) \
              .select("transaction_sk", "t.account_id", "transaction_date") \
              .show(10)
    print("⚠️  Unmatched rows will have NULL dim_account_key")
else:
    print("✅ All transactions resolved to a dim_account row")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Resolve `primary_customer_sk`
# 
# Bridge has `customer_id` not `customer_sk`.  
# Filter on `is_primary_holder = true` (more reliable than the `relationship_type` string).  
# Join bridge → `dim_customer` on `customer_id` to resolve `customer_sk`.

# CELL ********************

# bridge.account_id → silver_accounts.account_id → silver_accounts.customer_id (long) → dim_customer.customer_sk

silver_acct_keys = (
    spark.table(SILVER_ACCOUNTS)
    .select(
        F.col("account_id"),
        F.col("customer_id").alias("customer_id_long")
    )
)

dim_cust_keys = (
    spark.table(DIM_CUSTOMER)
    .select(
        F.col("customer_id"),
        F.col("customer_sk")
    )
)

bridge = (
    spark.table(BRIDGE)
    .filter(F.col("is_primary_holder") == True)
    .select(F.col("account_id"))
    # Step 1: account_id → long-format customer_id via silver accounts
    .join(silver_acct_keys, on="account_id", how="left")
    # Step 2: long-format customer_id → customer_sk via dim_customer
    .join(dim_cust_keys, on=(F.col("customer_id_long") == F.col("customer_id")), how="left")
    .select(
        F.col("account_id"),
        F.col("customer_sk").alias("primary_customer_sk")
    )
)

# Sanity: one primary per account
primary_dupes = bridge.groupBy("account_id").count().filter(F.col("count") > 1).count()
print(f"Accounts with >1 primary customer : {primary_dupes}")
if primary_dupes > 0:
    print("⚠️  Multiple primary customers per account — fanout risk")
    bridge.groupBy("account_id").count().filter(F.col("count") > 1).show(5)

joined_cust = (
    joined_acc.alias("f")
    .join(
        bridge.alias("b"),
        on=F.col("f.account_id") == F.col("b.account_id"),
        how="left"
    )
    .drop(F.col("b.account_id"))
)

unmatched_cust = joined_cust.filter(F.col("primary_customer_sk").isNull()).count()
print(f"Transactions with no primary_customer_sk : {unmatched_cust:,}")
if unmatched_cust == 0:
    print("✅ All transactions resolved to a primary customer")
else:
    pct = unmatched_cust / silver_count * 100
    print(f"⚠️  {pct:.2f}% of transactions have no primary customer — NULL FK retained")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Final Fact Columns

# CELL ********************

fact = (
    joined_cust

    # Signed measure: negative for debits
    .withColumn(
        "transaction_amount",
        F.when(F.col("is_debit"), F.col("amount") * -1)
         .otherwise(F.col("amount"))
         .cast(DoubleType())
    )

    # Absolute amount — always positive, for aggregation
    .withColumn(
        "transaction_amount_abs",
        F.abs(F.col("amount")).cast(DoubleType())
    )

    .select(

        # Surrogate PK 
        F.col("transaction_sk"),

        # Foreign keys
        F.col("date_key"),
        F.col("dim_account_key"),
        F.col("primary_customer_sk"),

        # Natural keys
        F.col("transaction_id"),
        F.col("account_id"),
        F.col("transaction_date"),
        F.col("transaction_timestamp"),

        # Measures
        F.col("transaction_amount"),
        F.col("transaction_amount_abs"),
        F.col("transaction_cost").alias("fee_amount"),
        F.col("authorization_time_ms"),

    
        F.col("channel"),
        F.col("category"),
        F.col("debit_credit"),
        F.col("currency"),
        F.col("description"),
        F.col("merchant_name"),
        F.col("loan_id"),
        F.col("do_debit_order_id"),
        F.col("rrn"),
        F.col("stan"),

        # Boolean flags
        F.col("is_debit"),
        F.col("is_credit"),
        F.col("is_debit_order"),
        F.col("is_loan_payment"),
        F.col("is_scheduled"),
        F.col("is_recurring"),
        F.col("is_salary_candidate"),
        F.col("is_initial_deposit"),
        F.col("has_error"),

        # Audit
        F.lit("lh_silver_banking_data.dbo.transactions").alias("record_source"),
        F.lit(GOLD_BATCH_ID).alias("gold_batch_id"),
        F.current_timestamp().alias("gold_load_timestamp"),
    )
)

fact_count = fact.count()
print(f"Fact rows assembled : {fact_count:,}")
print(f"Fact columns        : {len(fact.columns)}")
fact.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## FK Summary
# 
# Ran before writing to check all if all FKs are populated to an acceptable threshold.

# CELL ********************

fks = [
    "date_key",
    "dim_account_key",
    "primary_customer_sk",
]

print("=" * 55)
print("  FK COVERAGE CHECK")
print("=" * 55)
all_pass = True
for fk in fks:
    null_count = fact.filter(F.col(fk).isNull()).count()
    pct_null   = (null_count / fact_count * 100) if fact_count > 0 else 0
    flag = "✅" if pct_null < 1 else "⚠️ " if pct_null < 5 else "❌"
    if pct_null >= 5:
        all_pass = False
    print(f"  {flag}  {fk}: {null_count:,} nulls ({pct_null:.2f}%)")

print()
print(f"All FKs within threshold : {all_pass}")
print("=" * 55)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to `fact_transaction` — Delta Merge
# 
# 1. Merge key: `transaction_sk`  
# 2. On first run: full write.  
# 3. On incremental runs: upsert — new rows inserted, existing rows updated if source changed.

# CELL ********************

rows_inserted = 0
rows_updated  = 0

if not spark.catalog.tableExists(TARGET_TABLE):
    (
        fact.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE)
    )
    rows_inserted = fact_count
    print(f"✅ Created {TARGET_TABLE} (first run)")
    print(f"   Inserted : {rows_inserted:,}")

else:
    existing_keys = spark.table(TARGET_TABLE).select("transaction_sk")
    rows_inserted = fact.join(existing_keys, "transaction_sk", "left_anti").count()
    rows_updated  = fact_count - rows_inserted

    update_set = {
        c: f"s.{c}"
        for c in fact.columns
        if c not in ("transaction_sk", "gold_load_timestamp")
    }
    update_set["gold_load_timestamp"] = "current_timestamp()"

    (
        DeltaTable.forName(spark, TARGET_TABLE).alias("t")
        .merge(fact.alias("s"), "t.transaction_sk = s.transaction_sk")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"✅ Merged into {TARGET_TABLE}")
    print(f"   Inserted : {rows_inserted:,}")
    print(f"   Updated  : {rows_updated:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Update Gold Audit Log

# CELL ********************

END_TIME = datetime.datetime.utcnow()

(
    spark.createDataFrame([{
        "pipeline_name"   : PIPELINE_NAME,
        "batch_id"        : GOLD_BATCH_ID,
        "source_table"    : SOURCE_TABLE,
        "target_table"    : TARGET_TABLE,
        "rows_read"       : silver_count,
        "rows_inserted"   : rows_inserted,
        "rows_expired"    : 0,
        "start_timestamp" : START_TIME,
        "end_timestamp"   : END_TIME,
        "status"          : "SUCCESS",
    }])
    .write.format("delta").mode("append")
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

ft = spark.table(TARGET_TABLE)
total_rows = ft.count()

print("=" * 65)
print("  GOLD BUILD SUMMARY — fact_transaction")
print("=" * 65)
print(f"""
  Batch ID        : {GOLD_BATCH_ID}
  Source          : {SOURCE_TABLE}
  Target          : {TARGET_TABLE}
  Silver rows in  : {silver_count:,}
  Rows inserted   : {rows_inserted:,}
  Rows updated    : {rows_updated:,}
  Total fact rows : {total_rows:,}
""")

print("── Transaction amount stats ──")
ft.select(
    F.round(F.sum("transaction_amount_abs"), 2).alias("total_volume"),
    F.round(F.avg("transaction_amount_abs"), 2).alias("avg_amount"),
    F.round(F.min("transaction_amount_abs"), 2).alias("min_amount"),
    F.round(F.max("transaction_amount_abs"), 2).alias("max_amount"),
).show()

print("── Category distribution ──")
ft.groupBy("category").count().orderBy("count", ascending=False).show()

print("── Channel distribution ──")
ft.groupBy("channel").count().orderBy("count", ascending=False).show()

print("── Debit vs Credit ──")
ft.groupBy("debit_credit").count().orderBy("debit_credit").show()

print("── Flag summary ──")
flags = [
    "is_debit_order", "is_loan_payment", "is_scheduled",
    "is_recurring", "is_salary_candidate", "is_initial_deposit"
]
for flag in flags:
    n = ft.filter(F.col(flag) == True).count()
    print(f"  {flag}: {n:,}")

print()
print("── FK null check (final) ──")
for fk in ["date_key", "dim_account_key", "primary_customer_sk"]:
    n = ft.filter(F.col(fk).isNull()).count()
    flag = "✅" if n == 0 else "⚠️ "
    print(f"  {flag}  {fk}: {n:,} nulls")

print()
print("── Date range ──")
ft.agg(
    F.min("transaction_date").alias("earliest"),
    F.max("transaction_date").alias("latest")
).show()

print("── Sample rows ──")
display(
    ft.select(
        "transaction_sk", "date_key", "dim_account_key",
        "primary_customer_sk", "transaction_amount", "transaction_amount_abs",
        "fee_amount", "channel", "category", "debit_credit"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("fact_transaction") \
    .filter(F.col("transaction_amount_abs") == 0.0) \
    .groupBy("category", "channel") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
