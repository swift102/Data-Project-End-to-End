# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e1b0fd50-8d63-4667-998b-3fd590fa7ff9",
# META       "default_lakehouse_name": "lh_bronze_banking_data_modern_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "e1b0fd50-8d63-4667-998b-3fd590fa7ff9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Profiling
# 
# The purpose of this notebook is to perform data profiling and better understand the data that exists in the bronze table before moving to silver transformations. This is essential as it helps establish the relationships that exists in the different table and help when determining the different segments to move to when performing silver transformations


# MARKDOWN ********************

# ## Customers

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_customers")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# Check for a natural key candidate
for col in ["customer_id", "id", "customer_number"]:
    if col in df.columns:
        distinct = df.select(col).distinct().count()
        nulls    = df.filter(F.col(col).isNull()).count()
        print(f"Distinct {col}: {distinct:,}  |  Nulls: {nulls:,}")

# Null counts per column
print("\n=== NULL COUNTS ===")
null_counts = [(c, df.filter(F.col(c).isNull()).count()) for c in df.columns]
for col, n in null_counts:
    if n > 0:
        print(f"  {col}: {n:,} nulls")

print("\n===AGE ANALYSIS ===")


df.select(
    F.min("birth_date").alias("earliest_birth_date"),
    F.max("birth_date").alias("latest_birth_date")
).show(truncate=False)

customers_age = df.withColumn(
    "age",
    F.floor(F.months_between(F.current_date(), F.col("birth_date")) / 12)
)


print("\n===IMPOSSIBLE AGE ANALYSIS ===")
customers_age.filter(
    (F.col("age") < 0) |
    (F.col("age") > 120)
).select(
    "customer_id",
    "customer_type",
    "birth_date",
    "age"
).show(100, False)


print("\n===birth dates by customer type ANALYSIS ===")
df.groupBy("customer_type").agg(
    F.count("*").alias("total_records"),
    F.sum(
        F.when(F.col("birth_date").isNull(), 1).otherwise(0)
    ).alias("null_birth_dates")
).show(truncate=False)

print("\n===AGE ANALYSIS ===")
customers_age.agg(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("birth_date").isNull(), 1).otherwise(0)).alias("null_birth_dates"),
    F.sum(F.when(F.col("birth_date") > F.current_date(), 1).otherwise(0)).alias("future_birth_dates"),
    F.sum(F.when(F.col("age") < 0, 1).otherwise(0)).alias("negative_ages"),
    F.sum(F.when(F.col("age") > 120, 1).otherwise(0)).alias("ages_over_120")
).show()


print("\n===AGE FILTER ===")
suspect = customers_age.filter(
    (F.col("age") < 0) |
    (F.col("age") > 120)
)

suspect = suspect.withColumn(
    "fixed_birth_date",
    F.when(
        F.year("birth_date") > 2030,
        F.add_months("birth_date", -960)
    )
    .when(
        F.year("birth_date") < 1910,
        F.add_months("birth_date", 960)
    )
    .otherwise(F.col("birth_date"))
)

suspect.withColumn(
    "fixed_age",
    F.floor(
        F.months_between(
            F.current_date(),
            F.col("fixed_birth_date")
        ) / 12
    )
).select(
    "customer_id",
    "birth_date",
    "age",
    "fixed_birth_date",
    "fixed_age"
).show(50, False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customers_age.filter(
    (F.col("age") < 0) |
    (F.col("age") > 120)
).select(
    F.year("birth_date").alias("birth_year")
).groupBy("birth_year").count().orderBy("birth_year").show(200, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

check = (
    customers_age
    .filter((F.col("age") < 0) | (F.col("age") > 120))
    .withColumn("birth_year", F.year("birth_date"))
    .withColumn("minus_80_years", F.year(F.add_months("birth_date", -960)))
    .withColumn("plus_80_years", F.year(F.add_months("birth_date", 960)))
)

check.select(
    "birth_year",
    "minus_80_years",
    "plus_80_years"
).show(50, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suspect = customers_age.filter(
    (F.col("age") < 0) |
    (F.col("age") > 120)
)

suspect = suspect.withColumn(
    "fixed_birth_date",
    F.when(
        F.year("birth_date") > 2030,
        F.add_months("birth_date", -960)
    )
    .when(
        F.year("birth_date") < 1910,
        F.add_months("birth_date", 960)
    )
    .otherwise(F.col("birth_date"))
)

suspect.withColumn(
    "fixed_age",
    F.floor(
        F.months_between(
            F.current_date(),
            F.col("fixed_birth_date")
        ) / 12
    )
).select(
    "customer_id",
    "birth_date",
    "age",
    "fixed_birth_date",
    "fixed_age"
).show(50, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # bronze_account_signatories

# CELL ********************

sig = spark.table("bronze_account_signatories")
customers = spark.table("bronze_customers").select("customer_id")

orphans = sig.join(customers, "customer_id", "left_anti")

print(f"Orphan rows: {orphans.count():,}")
orphans.groupBy("year", "month").count().orderBy("year", "month").show(20)
orphans.groupBy("signatory_role").count().show()
orphans.groupBy("_batch_id").count().orderBy(F.desc("count")).show(10)

orphans.select("customer_id", "signatory_role").distinct().show(10, truncate=False)

# Compare format to a normal customer_id
spark.table("bronze_customers").select("customer_id").show(5, truncate=False)

# Check the 10 stray primary_holder orphans specifically — these are the odd ones out
orphans.filter(F.col("signatory_role") == "primary_holder").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Accounts

# CELL ********************

df = spark.table("bronze_accounts")
df.printSchema()
print(df.count())
# then null counts + distinct on account_id + sample

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_accounts")

print("=== KEY DISTINCT COUNTS ===")
for col in ["account_id", "customer_id", "account_type", 
            "account_status", "account_tier", "account_purpose",
            "card_type", "currency", "cdc_op_hint"]:
    if col in df.columns:
        vals = [r[0] for r in df.select(col).distinct().collect()]
        print(f"\n  {col} ({len(vals)} distinct):")
        for v in sorted([str(v) for v in vals])[:15]:
            print(f"    {v}")

print("\n=== NULL COUNTS (non-zero only) ===")
for col in df.columns:
    n = df.filter(F.col(col).isNull()).count()
    if n > 0:
        print(f"  {col}: {n:,}")

print("\n=== closure_date sample (checking integer type) ===")
df.select("closure_date").filter(F.col("closure_date").isNotNull()).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_accounts")

accts_per_customer = (df.groupBy("customer_id")
   .agg(F.count("*").alias("n_accounts")))

(accts_per_customer
   .groupBy("n_accounts")
   .agg(F.count("*").alias("n_customers"))
   .orderBy("n_accounts")
   .show(20))

print("\n=== closure_date sample ===")
df.select("closure_date").filter(F.col("closure_date").isNotNull()).show(10)

print("\n=== account_status distribution ===")
df.groupBy("account_status").count().orderBy("count", ascending=False).show()

print("\n=== account_tier x account_type cross ===")
df.groupBy("account_tier", "account_type").count().orderBy("account_tier","account_type").show(30)

print("\n=== cdc_op_hint distribution ===")
df.groupBy("cdc_op_hint").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_accounts")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Customer communication complaints

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_customer_communications_complaints")

print("\n==============================")
print("  CUSTOMER COMPLAINTS PROFILING")
print("==============================")


# BASIC OVERVIEW

print("\n=== SCHEMA ===")
df.printSchema()

total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")


# KEY UNIQUENESS CHECK

print("\n=== KEY ANALYSIS ===")
for col in ["comm_id", "account_id", "customer_id"]:
    if col in df.columns:
        print(
            f"{col} -> distinct: {df.select(col).distinct().count():,} | "
            f"nulls: {df.filter(F.col(col).isNull()).count():,}"
        )

# NULL PROFILING

print("\n=== NULL COUNTS (non-zero only) ===")
nulls = df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
]).collect()[0].asDict()

for k, v in nulls.items():
    if v > 0:
        print(f"{k}: {v:,}")


# COMPLAINT DISTRIBUTION

print("\n=== IS COMPLAINT DISTRIBUTION ===")
df.groupBy("is_complaint").count().show()

print("\n=== COMPLAINT CATEGORY ===")
df.groupBy("complaint_category") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(10, False)

print("\n=== RESOLUTION STATUS ===")
df.groupBy("resolution_status") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(10, False)


# SENTIMENT ANALYSIS

print("\n=== SENTIMENT ===")
df.groupBy("sentiment").count().show()

print("\n=== COMPLAINT vs SENTIMENT ===")
df.groupBy("is_complaint", "sentiment") \
  .count() \
  .show()


# CHANNEL & DIRECTION

print("\n=== CHANNEL ===")
df.groupBy("channel") \
  .count() \
  .orderBy(F.desc("count")) \
  .show()

print("\n=== DIRECTION ===")
df.groupBy("direction") \
  .count() \
  .show()


# TIMESTAMP QUALITY

df_ts = df.withColumn("comm_ts", F.to_timestamp("timestamp"))

print("\n=== TIMESTAMP PARSING ISSUES ===")
print("Null parsed timestamps:",
      df_ts.filter(F.col("comm_ts").isNull()).count())

print("\n=== MONTHLY TRENDS ===")
df_ts.groupBy(
    F.year("comm_ts").alias("year"),
    F.month("comm_ts").alias("month")
).count() \
 .orderBy("year", "month") \
 .show(20)


# CUSTOMER BEHAVIOUR

print("\n=== TOP COMPLAINING CUSTOMERS ===")
df.filter(F.col("is_complaint") == True) \
  .groupBy("customer_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(10)

print("\n=== TOP COMPLAINING ACCOUNTS ===")
df.filter(F.col("is_complaint") == True) \
  .groupBy("account_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(10)


# CUSTOMER RISK SIGNAL
print("\n=== CUSTOMER RISK SIGN ===")
df.groupBy("customer_id") \
  .agg(F.count("*").alias("complaint_count")) \
  .orderBy(F.desc("complaint_count")) \
  .withColumn(
      "risk_band",
      F.when(F.col("complaint_count") >= 8, "HIGH")
       .when(F.col("complaint_count") >= 4, "MEDIUM")
       .otherwise("LOW")
  ).show()

# DATA CONSISTENCY CHECK
## Complaint should match sentiment
print("\n=== Customer sentiment check  ===")
df.filter(
    (F.col("is_complaint") == True) &
    (F.col("sentiment") == "Positive")
).count()

print("\n=== Duplicates check  ===")
df.groupBy("customer_id", "subject", "body", "timestamp") \
  .count() \
  .filter("count > 1") \
  .count()

# COMPLAINT RATE
complaint_rate = df.filter(F.col("is_complaint") == True).count() / df.count()
print(f"Complaint rate: {complaint_rate:.2%}")

# REFERENTIAL INTEGRITY CHECKS

print("\n=== REFERENTIAL INTEGRITY ===")

customers = spark.table("lh_silver_banking_data.dbo.customers_individual")
accounts = spark.table("lh_silver_banking_data.dbo.accounts")  

missing_customers = df.join(customers, "customer_id", "left_anti").count()
missing_accounts = df.join(accounts, "account_id", "left_anti").count()

print(f"Missing customers in dimension: {missing_customers:,}")
print(f"Missing accounts in dimension : {missing_accounts:,}")

print("\n=== Timestamp ===")
df.filter(F.to_timestamp("timestamp").isNull()) \
  .select("timestamp") \
  .distinct() \
  .show(100, False)

# TEXT QUALITY

print("\n=== TEXT PROFILING ===")

df.select(
    F.min(F.length("subject")).alias("min_subject_len"),
    F.max(F.length("subject")).alias("max_subject_len"),
    F.avg(F.length("subject")).alias("avg_subject_len"),
    F.min(F.length("body")).alias("min_body_len"),
    F.max(F.length("body")).alias("max_body_len"),
    F.avg(F.length("body")).alias("avg_body_len")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = spark.sql("SHOW TABLES")

for row in tables.collect():
    print(row.tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  bronze_collections_cases_collections_cases
# 
# ## DQ Investigation: `collections_cases` — Customer/Account Key Format Inconsistency
# 
# ### Summary
# 
# Bronze source data uses an 11-character customer ID format (e.g. `IND19012454`, `COM21000053`), while the Silver `accounts` and `customers_individual` tables use a 13-character format (e.g. `IND1901002572`). This format divergence was introduced during the Bronze → Silver transform and is the root cause of the in-progress 81K/119K customer key migration.
# 
# ### Impact on `collections_cases`
# 
# - **`customer_id` FK vs `customers_individual`**: all 12,235/12,235 rows initially failed to join, due to the 11-char vs 13-char format mismatch — not a data quality issue with `collections_cases` itself.
# - **`customer_id` FK vs `bronze_customers`** (correct comparison point, after trimming whitespace): clean. Only **55/12,235 (0.45%)** customer_ids do not exist in `bronze_customers` at all — all 2019-vintage (53 `IND`, 2 `COM` prefixes). Logged as known limitation **DQ-COLLECTIONS-001** (likely a 2019 ingestion gap).
# - **`account_id` FK vs Silver `accounts`**: 259/12,235 orphans — pending verification against Bronze. Likely also a format-mismatch false positive, since `accounts.customer_id` shares the same 13-char anomaly.
# 
# ### Table Format Audit (for migration spec)
# 
# | Table | `customer_id` format | Row count | Status |
# |---|---|---|---|
# | `bronze_collections_cases` | 11-char | 12,226 / 12,235 | ✅ Correct (matches Bronze source) |
# | `bronze_customers` | 11-char | 117,889 | ✅ Reference / correct format |
# | Silver `accounts` | 13-char | 109,839 / 109,849 | ⚠️ Wrong format — same issue as `customers_individual` |
# | Silver `customers_individual` | 13-char | 116,536 | ⚠️ Wrong format — subject of ongoing migration |
# | Silver `bridge_customer_account` | 11-char (mostly) | 118,541 / 121,247 | ⚠️ 2,706 rows at 10-char — separate anomaly, needs investigation |
# 
# ### Other DQ Findings — All Clean
# 
# - `case_id`: unique, no nulls (12,235/12,235).
# - Date ranges sane, no future dates:
#   - `last_contact_date`: 2019-02-01 to 2025-12-31
#   - `promise_to_pay_date`: 2019-02-05 to 2026-01-12
# - `arrangement_plan` correctly restricted to `open`/`resolved` statuses (0 in `closed`).
# - `collection_stage` bands align cleanly with `days_past_due`:
#   - `pre_delinquent`: 1–30
#   - `early_collections`: 31–60
#   - `late_collections`: 61–90
#   - `legal`: 91–120
#   - `write_off`: 121–365
# - Categorical fields clean: `last_contact_channel` (6 distinct, 0 nulls), `assigned_collector` (10 distinct, 0 nulls).
# - Multiple open cases per account/customer (up to 5) — expected, not a defect.
# 
# ### Conclusion / Next Steps
# 
# `200_005` Silver transform for `collections_cases` is **blocked pending the customer/account key migration**, which must rebuild `accounts` and `customers_individual` (and possibly correct the 2,706-row `bridge_customer_account` anomaly) to the 11-char Bronze-aligned format before FK joins from `collections_cases` will resolve correctly.
# 
# Once the migration lands, re-run the FK checks. Expected residual gap:
# - ~55 rows for `customer_id` (DQ-COLLECTIONS-001)
# - remaining portion of the 259 `account_id` orphans after format correction (TBD)


# MARKDOWN ********************

# ## DQ Investigation: `collections_cases` — UPDATE (2026-06-19)
# 
# ### Context
# 
# Original DQ investigation (above) was run against a `collections_cases` snapshot of 12,235 rows. Current Bronze snapshot has grown to **12,809 rows** (+574, new batch landed). Re-running FK and consistency checks against this snapshot surfaces two changes from the original findings — both need to be tracked before this notebook is unblocked.
# 
# ### 1. Customer orphan rate increased — DQ-COLLECTIONS-001 baseline is stale
# 
# True orphan check (against `bronze_customers`/`bronze_accounts`, 11-char format — correct comparison point):
# 
# | FK | Orphan count | Orphan rate | Original baseline |
# |---|---|---|---|
# | `customer_id` | 160 / 12,809 | **1.25%** | 0.45% (55/12,235) |
# | `account_id` | 284 / 12,809 | **2.22%** | ~2.1% (259/12,235, estimated) |
# 
# - `account_id` orphan rate is consistent with the original estimate — confirms this gap is **real**, not primarily a format-mismatch false positive as originally speculated.
# - `customer_id` orphan rate nearly **tripled** (0.45% → 1.25%). Row count only grew ~4.7%, so this is not explained by volume alone — the new batch appears to have introduced additional unmatched `customer_id`s.
# 
# **Action needed:** identify which `customer_id`s in the new batch fail to match `bronze_customers`, and whether they share the 2019-vintage pattern of the original DQ-COLLECTIONS-001 finding or represent a new gap. Until classified, do not assume this is the same known limitation.
# 
# ### 2. `arrangement_plan` now appears on `closed` cases — contradicts original finding
# 
# Original doc stated `arrangement_plan` is correctly restricted to `open`/`resolved` statuses, with 0 rows in `closed`. Current snapshot:
# 
# | status | rows with `arrangement_plan` set |
# |---|---|
# | `closed` | **154** |
# | `open` | 544 |
# | `resolved` | 464 |
# 
# This is a logical-consistency change, not a rerun artifact. Possible explanations:
# - New batch includes cases that had an active arrangement when closed (plausible business state — arrangement existed, then case was closed/written off)
# - Upstream Bronze logic changed between batches
# - Original check was run against a different/stale snapshot
# 
# **Action needed:** confirm with business/source-system owner whether `arrangement_plan` + `closed` is a valid combination. If valid, update the Silver DQ rule documentation. If invalid, flag as a new DQ ticket (suggest **DQ-COLLECTIONS-002**) and decide whether to null out `arrangement_plan` for closed cases in Silver or pass through as-is with a flag.
# 
# ### 3. Confirmed clean (re-validated)
# 
# - `notes` field: 0 nulls, length 59–196 chars, no PII pattern hits (ID numbers, phone numbers, emails) on regex scan.
# - `assigned_collector` load distribution: balanced across 10 collectors (1,225–1,355 cases each), no skew; write-off rates even (53–76 per collector).
# 
# ### Revised Conclusion
# 
# `200_005` remains **blocked pending the customer/account key migration** for the reasons in the original doc, but the orphan-rate baseline used for any unblock guard must be updated:
# - Customer threshold should reflect the new 1.25% true rate (not the stale 0.45%) until DQ-COLLECTIONS-001 is re-classified against this batch
# - Account orphan rate (2.22%) should be treated as a genuine, not format-driven, gap going forward
# 
# New open item: **DQ-COLLECTIONS-002** — `arrangement_plan` present on `closed` status cases, pending business rule confirmation.


# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_collections_cases_collections_cases")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# Check for a natural key candidate
for col in ["case_id", "id", "case_number"]:
    if col in df.columns:
        distinct = df.select(col).distinct().count()
        nulls    = df.filter(F.col(col).isNull()).count()
        print(f"Distinct {col}: {distinct:,}  |  Nulls: {nulls:,}")

# Null counts per column
print("\n=== NULL COUNTS ===")
null_counts = [(c, df.filter(F.col(c).isNull()).count()) for c in df.columns]
for col, n in null_counts:
    if n > 0:
        print(f"  {col}: {n:,} nulls")

df.groupBy("status").agg(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("promise_to_pay_amount").isNull(), 1).otherwise(0)).alias("null_ptp")
).show(truncate=False)

df.filter(F.col("arrangement_plan").isNotNull()) \
  .groupBy("arrangement_plan") \
  .count() \
  .show(truncate=False)

df.groupBy("status") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(50, False)

df.groupBy("collection_stage") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(50, False)

df.select(
    F.min("days_past_due").alias("min"),
    F.max("days_past_due").alias("max"),
    F.avg("days_past_due").alias("avg")
).show()

df.select(
    F.min("arrears_amount").alias("min"),
    F.max("arrears_amount").alias("max"),
    F.avg("arrears_amount").alias("avg")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. FK integrity vs Silver dimensions
silver_accounts = spark.table("lh_silver_banking_data.dbo.accounts")
silver_customers = spark.table("lh_silver_banking_data.dbo.customers_individual")

orphan_accounts = df.join(silver_accounts.select("account_id"), "account_id", "left_anti").count()
orphan_customers = df.join(silver_customers.select("customer_id"), "customer_id", "left_anti").count()
print(f"Orphan account_id  : {orphan_accounts:,}")
print(f"Orphan customer_id : {orphan_customers:,}")

# 2. Date range sanity
df.select(
    F.min("last_contact_date").alias("min_contact"),
    F.max("last_contact_date").alias("max_contact"),
    F.min("promise_to_pay_date").alias("min_ptp"),
    F.max("promise_to_pay_date").alias("max_ptp")
).show()

# Future-dated checks
today = F.current_date()
future_contact = df.filter(F.col("last_contact_date") > today).count()
future_ptp = df.filter(F.col("promise_to_pay_date") > today).count()
print(f"Future last_contact_date : {future_contact:,}")
print(f"Future promise_to_pay_date: {future_ptp:,}")

# 3. Logical consistency: status vs PTP fields
df.groupBy("status").agg(
    F.sum(F.when(F.col("promise_to_pay_amount").isNotNull(), 1).otherwise(0)).alias("has_ptp"),
    F.count("*").alias("total")
).show()

# arrangement_plan should only appear for open/resolved (hypothesis check)
df.filter(F.col("arrangement_plan").isNotNull()).groupBy("status").count().show()

# 4. collection_stage vs status cross-tab
df.groupBy("collection_stage", "status").count().orderBy("collection_stage", "status").show(50, False)

# collection_stage vs days_past_due ranges (sanity check ordering)
df.groupBy("collection_stage").agg(
    F.min("days_past_due").alias("min_dpd"),
    F.max("days_past_due").alias("max_dpd"),
    F.avg("days_past_due").alias("avg_dpd")
).orderBy("min_dpd").show()

# write_off should generally have high dpd / closed status
df.filter(F.col("collection_stage") == "write_off").groupBy("status").agg(
    F.min("days_past_due").alias("min_dpd"),
    F.max("days_past_due").alias("max_dpd")
).show()

# 5. Categorical cardinality for masking/lookup design
for col in ["last_contact_channel", "assigned_collector"]:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col}: {distinct:,} distinct | {nulls:,} nulls")

df.groupBy("last_contact_channel").count().orderBy(F.desc("count")).show()

# 6. Duplicate (account_id, customer_id) cases — multiple open cases per account
df.filter(F.col("status") == "open") \
  .groupBy("account_id", "customer_id") \
  .count() \
  .filter(F.col("count") > 1) \
  .show(20, False)

# 7. _commit_sha / _batch_id / _ingest_timestamp sanity (lineage check)
df.select(
    F.countDistinct("_batch_id").alias("distinct_batches"),
    F.countDistinct("_commit_sha").alias("distinct_commits"),
    F.min("_ingest_timestamp").alias("min_ts"),
    F.max("_ingest_timestamp").alias("max_ts")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# === TRUE ORPHAN CHECK (vs Bronze, correct 11-char format) ===
bronze_customers = spark.table("lh_bronze_banking_data_modern_data.dbo.bronze_customers")
bronze_accounts = spark.table("lh_bronze_banking_data_modern_data.dbo.bronze_accounts")

true_orphan_customer = df.join(
    bronze_customers.select(F.trim(F.col("customer_id")).alias("_cid")),
    F.trim(df.customer_id) == F.col("_cid"), "left_anti"
).count()

true_orphan_account = df.join(
    bronze_accounts.select(F.trim(F.col("account_id")).alias("_aid")),
    F.trim(df.account_id) == F.col("_aid"), "left_anti"
).count()

total = df.count()
print(f"TRUE orphan customer_id (vs Bronze): {true_orphan_customer:,} / {total:,} ({true_orphan_customer/total:.2%})")
print(f"TRUE orphan account_id  (vs Bronze): {true_orphan_account:,} / {total:,} ({true_orphan_account/total:.2%})")

# === notes field profiling ===
df.select(
    F.count(F.when(F.col("notes").isNull(), 1)).alias("null_notes"),
    F.avg(F.length("notes")).alias("avg_len"),
    F.max(F.length("notes")).alias("max_len"),
    F.min(F.length("notes")).alias("min_len")
).show()

# quick eyeball for PII leakage in free text (ID numbers, phone patterns, emails)
df.filter(
    F.col("notes").rlike(r"\d{13}") |              # SA ID number length
    F.col("notes").rlike(r"\d{3}[\s-]?\d{3}[\s-]?\d{4}") |  # phone-ish
    F.col("notes").rlike(r"[\w\.-]+@[\w\.-]+")      # email
).select("case_id", "notes").show(20, truncate=80)

# === assigned_collector load distribution ===
df.groupBy("assigned_collector").agg(
    F.count("*").alias("case_count"),
    F.avg("days_past_due").alias("avg_dpd"),
    F.sum(F.when(F.col("collection_stage") == "write_off", 1).otherwise(0)).alias("write_offs")
).orderBy(F.desc("case_count")).show(20, truncate=False)

# === arrangement_plan only for open/resolved? ===
df.filter(F.col("arrangement_plan").isNotNull()).groupBy("status").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Account signatories


# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_account_signatories")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# Check for a natural key candidate
for col in ["account_id", "id", "account_number"]:
    if col in df.columns:
        distinct = df.select(col).distinct().count()
        nulls    = df.filter(F.col(col).isNull()).count()
        print(f"Distinct {col}: {distinct:,}  |  Nulls: {nulls:,}")

# Null counts per column
print("\n=== NULL COUNTS ===")
null_counts = [(c, df.filter(F.col(c).isNull()).count()) for c in df.columns]
for col, n in null_counts:
    if n > 0:
        print(f"  {col}: {n:,} nulls")

# === CUSTOMER_ID CARDINALITY CHECK ===
print("=== CUSTOMER_ID PROFILE ===")
distinct_cust = df.select("customer_id").distinct().count()
print(f"Distinct customer_id : {distinct_cust:,}")
print(f"Total rows           : {df.count():,}")

# Customers with multiple signatory entries
cust_freq = (
    df.groupBy("customer_id")
      .agg(F.count("*").alias("signatory_count"))
      .groupBy("signatory_count")
      .agg(F.count("*").alias("num_customers"))
      .orderBy("signatory_count")
)
print("\n=== SIGNATORY COUNT DISTRIBUTION (per customer_id) ===")
cust_freq.show(20)

# === ACCOUNT_ID FAN-OUT (accounts with multiple signatories) ===
print("\n=== ACCOUNT_ID FAN-OUT ===")
acct_freq = (
    df.groupBy("account_id")
      .agg(F.count("*").alias("signatory_count"))
      .groupBy("signatory_count")
      .agg(F.count("*").alias("num_accounts"))
      .orderBy("signatory_count")
)
acct_freq.show(20)

# === SIGNATORY_ROLE / SIGNING_RULE VALUE DISTRIBUTION ===
print("\n=== SIGNATORY_ROLE VALUES ===")
df.groupBy("signatory_role").count().orderBy(F.desc("count")).show(20, truncate=False)

print("\n=== SIGNING_RULE VALUES ===")
df.groupBy("signing_rule").count().orderBy(F.desc("count")).show(20, truncate=False)

# === IS_ACTIVE BREAKDOWN ===
print("\n=== IS_ACTIVE ===")
df.groupBy("is_active").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_account_product_enrollments

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_account_product_enrollments")

print("=" * 50)
print("ACCOUNT PRODUCT ENROLLMENTS PROFILING")
print("=" * 50)

# Schema
print("\n=== SCHEMA ===")
df.printSchema()

# Row count
total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")

# Distincts & nulls
print("\n=== KEY ANALYSIS ===")
for col in df.columns:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col:30} Distinct={distinct:,}  Nulls={nulls:,}")

# Duplicate account-product combinations
if {"account_id", "product_name"}.issubset(df.columns):
    print("\n=== DUPLICATE ENROLLMENTS ===")
    (
        df.groupBy("account_id", "product_name")
          .count()
          .filter(F.col("count") > 1)
          .show(20, False)
    )

# Null counts
print("\n=== NULL COUNTS ===")
(
    df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])
    .show(truncate=False)
)


# Product distribution
df.groupBy("product_code").count().orderBy(F.desc("count")).show()

# Status distribution
df.groupBy("enrollment_status").count().show()

# Duplicate account-product pairs
df.groupBy("account_id","product_code") \
  .count() \
  .filter("count > 1") \
  .show()

df.groupBy("account_id") \
  .count() \
  .orderBy(F.desc("count")) \
  .show(10)

accounts = spark.table(
    "lh_silver_banking_data.dbo.accounts"
)

missing_accounts = (
    df.join(
        accounts.select("account_id"),
        "account_id",
        "left_anti"
    ).count()
)

# Products per account
df.groupBy("account_id") \
  .agg(F.count("*").alias("product_count")) \
  .groupBy("product_count") \
  .count() \
  .orderBy("product_count") \
  .show()

print(missing_accounts)

# Sample records
print("\n=== SAMPLE RECORDS ===")
df.show(5)

print("\n=== CHECK FOR UNIQUENESS ===")
candidates = [
    ["account_id", "enrollment_date"],
    ["account_id", "enrollment_date", "product_code"],
    ["account_id", "enrollment_date", "enrollment_status"],
]

total = df.count()
for key in candidates:
    distinct = df.select(*key).distinct().count()
    print(f"{key} -> distinct={distinct:,} / total={total:,} -> {'UNIQUE' if distinct == total else 'DUPES'}")

df.select("product_code").distinct().show()
df.select("enrollment_status").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_account_limits_history

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_account_limits_history")
print("=" * 50)
print("ACCOUNT LIMITS HISTORY PROFILING")
print("=" * 50)

# Schema
print("\n=== SCHEMA ===")
df.printSchema()

# Row count
total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")

# Distincts & nulls
print("\n=== KEY ANALYSIS ===")
for col in df.columns:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col:30} Distinct={distinct:,}  Nulls={nulls:,}")

print("\n=== CHECK FOR UNIQUENESS ===")
candidates = [
    ["account_id", "event_date"],
    ["account_id", "event_date", "event_type"],
    ["account_id", "event_date", "change_reason"],
]

total = df.count()
for key in candidates:
    distinct = df.select(*key).distinct().count()
    print(f"{key} -> distinct={distinct:,} / total={total:,} -> {'UNIQUE' if distinct == total else 'DUPES'}")

df.select("event_type").distinct().show()
df.select("change_reason").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_account_status_events

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_account_status_events")

print("=" * 50)
print("ACCOUNT STATUS PROFILING")
print("=" * 50)

# Schema
print("\n=== SCHEMA ===")
df.printSchema()

# Row count
total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")

# Distincts & nulls
print("\n=== KEY ANALYSIS ===")
for col in df.columns:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col:30} Distinct={distinct:,}  Nulls={nulls:,}")

# Test candidate keys for uniqueness
candidates = [
    ["account_id", "event_date"],
    ["account_id", "event_date", "event_type"],
    ["account_id", "event_date", "event_type", "new_status"],
    ["account_id", "event_date", "status_reason"],
]

total = df.count()
for key in candidates:
    distinct = df.select(*key).distinct().count()
    print(f"{key} -> distinct={distinct:,} / total={total:,} -> {'UNIQUE' if distinct == total else 'DUPES'}")

# If all still show dupes, inspect actual duplicate rows on the most likely key
df.groupBy("account_id", "event_date", "event_type").count() \
  .filter("count > 1") \
  .orderBy(F.desc("count")) \
  .show(10, truncate=False)


df.select("event_type").distinct().show()
df.select("new_status").distinct().show()
df.select("status_reason").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_collections_cases_recovery_payments

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_collections_cases_recovery_payments")

print("=" * 50)
print("ACCOUNT STATUS PROFILING")
print("=" * 50)

# Schema
print("\n=== SCHEMA ===")
df.printSchema()

# Row count
total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")

# Distincts & nulls
print("\n=== KEY ANALYSIS ===")
for col in df.columns:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col:30} Distinct={distinct:,}  Nulls={nulls:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_customer_communications_communications

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_customer_communications_communications")

print("=" * 50)
print("ACCOUNT STATUS PROFILING")
print("=" * 50)

# Schema
print("\n=== SCHEMA ===")
df.printSchema()

# Row count
total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")

# Distincts & nulls
print("\n=== KEY ANALYSIS ===")
for col in df.columns:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col:30} Distinct={distinct:,}  Nulls={nulls:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transaction 

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_transactions")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_txn = df.select("transaction_id").distinct().count()
null_txn     = df.filter(F.col("transaction_id").isNull()).count()
print(f"Distinct transaction_id : {distinct_txn:,}  |  Nulls: {null_txn:,}")
print(f"Total rows              : {df.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.filter(F.col("debit_order_id").isNotNull()) \
  .select(
      "debit_order_id",
      "debit_order_metadata.debit_order_id",
      "debit_order_type",
      "debit_order_metadata.debit_order_type"
  ).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# How many transaction_ids have duplicates, and how many copies?
dup_dist = (
    df.groupBy("transaction_id")
      .count()
      .filter(F.col("count") > 1)
)

print(f"transaction_ids with duplicates : {dup_dist.count():,}")
dup_dist.groupBy("count").agg(F.count("*").alias("n_transactions")) \
        .orderBy("count").show()

# Are duplicates explained by year/month (monthly snapshots like customers)?
df.filter(F.col("transaction_id").isin(
    dup_dist.limit(5).select("transaction_id").rdd.flatMap(lambda x: x).collect()
)).select("transaction_id", "transaction_date", "year", "month", 
          "record_last_updated_at", "_ingest_timestamp") \
  .orderBy("transaction_id") \
  .show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for col in ["status", "debit_credit", "channel", "category", "currency"]:
    print(f"\n── {col} ──")
    df.groupBy(col).count().orderBy("count", ascending=False).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select("transaction_date", "transaction_time", "transaction_timestamp") \
  .show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # bronze_debit_orders

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, date_format
from pyspark.sql.functions import col

df = spark.table("bronze_debit_orders")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")


df.filter(F.col("debit_order_id").isNotNull()) \
  .select(
      "debit_order_id",
      "debit_order_type"
  ).show(10, truncate=False)

# Distinct debit_order_id
distinct_do = df.select("debit_order_id").distinct().count()
null_do     = df.filter(F.col("debit_order_id").isNull()).count()
print(f"Distinct debit_order_id : {distinct_do:,}  |  Nulls: {null_do:,}")
print(f"Total rows              : {df.count():,}")


# Null Analysis
print("=== Null Analysis ===")
total_rows = df.count()

null_stats = df.select([
    (
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
        / F.lit(total_rows) * 100
    ).alias(c)
    for c in df.columns
])

display(null_stats)

print("=== Profiles ===")
def profile_categorical(df, columns, top_n=10):
    for c in columns:
        print(f"\n--- {c} ---")
        (
            df.groupBy(c)
              .count()
              .orderBy(F.desc("count"))
              .show(top_n, truncate=False)
        )
# Debit Orders
profile_categorical(df, ["debit_order_type", "frequency", "status", "notification_method"])


# Date Profiling
print("=== Date Profiling===")
def date_profile(df, date_cols, table_name):
    print(f"\n=== DATE PROFILE: {table_name} ===")

    for dc in date_cols:
        print(f"\n--- {dc} ---")

        (
            df.groupBy(
                F.year(F.col(dc)).alias("year"),
                F.month(F.col(dc)).alias("month")
            )
            .count()
            .orderBy("year", "month")
            .show(20)
        )

date_profile = ["start_date", "end_date", "cancellation_date", "record_last_updated_at"]

# Duplicate & Key Quality Analysis

print("=== Duplicate & Key Quality Analysis ===")
# check for duplicates beyond natural key
df.groupBy("debit_order_id").count().filter("count > 1").count()  # should be 0?


print("=== Debit orders referencing loans ===")

df_loans = spark.table("bronze_loans")

# Debit orders referencing loans
linked_loans = df.filter(F.col("linked_loan_id").isNotNull())
print(f"Debit orders with linked_loan_id: {linked_loans.count():,}")

missing_loans = linked_loans.join(df_loans, 
                                  linked_loans.linked_loan_id == df_loans.loan_id, 
                                  "left_anti")
print(f"Linked loan_ids missing in bronze_loans: {missing_loans.count():,}")
missing_loans.select("linked_loan_id").distinct().show(10)

print("=== update patternss ===")

df.groupBy("debit_order_id").agg(
    F.count("*").alias("versions"),
    F.min("record_last_updated_at").alias("first_seen"),
    F.max("record_last_updated_at").alias("last_updated")
).orderBy(F.desc("versions")).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # bronze_loans

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import year, month, date_format
from pyspark.sql.functions import col

df = spark.table("bronze_loans")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# Distinct loan_id
distinct_loan = df.select("loan_id").distinct().count()
null_do     = df.filter(F.col("loan_id").isNull()).count()
print(f"Distinct loan_id : {distinct_loan:,}  |  Nulls: {null_do:,}")
print(f"Total rows              : {df.count():,}")


# Null Analysis
print("=== Null Analysis ===")
total_rows = df.count()

null_stats = df.select([
    (
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
        / F.lit(total_rows) * 100
    ).alias(c)
    for c in df.columns
])

display(null_stats)


print("=== Profiles ===")
def profile_categorical(df, columns, top_n=10):
    for c in columns:
        print(f"\n--- {c} ---")
        df.groupBy(c).count() \
          .orderBy(col("count").desc()) \
          .show(top_n, truncate=False)

# Loans
profile_categorical(df, ["loan_type", "application_status", "workflow_state", 
                               "rate_type", "collateral_type", "pricing_basis"])

# Date Profiling
print("=== Date Profiling===")
def date_profile(df, date_cols, table_name):
    print(f"\n=== DATE PROFILE: {table_name} ===")
    for dc in date_cols:
        print(f"\n--- {dc} ---")
        df.groupBy(year(dc).alias("year"), month(dc).alias("month")) \
          .count().orderBy("year","month").show(20)

date_profile(df, ["application_date", "booked_at", "disbursed_at", "decision_at"], "Loans")


# Duplicate & Key Quality Analysis

print("=== Duplicate & Key Quality Analysis ===")
# check composite keys if needed
df.groupBy("loan_id", "customer_id").count().filter("count > 1").show()

print("=== amount_granted > requested_amount? ===")
df.filter("amount_granted > requested_amount * 1.1").count()
df.filter("discretionary_income < 0").count()

print("=== Customer Level Aggregation ===")
df.groupBy("customer_id").agg(
    F.count("loan_id").alias("num_loans"),
    F.sum("amount_granted").alias("total_exposure")
).orderBy(
    F.col("num_loans").desc()
).show(20)

print("=== Application ===")
df.groupBy("application_status").agg(
    F.count("*").alias("loans"),
    F.avg("amount_granted").alias("avg_amount"),
    F.sum("amount_granted").alias("total_amount")
).show()

# Logical consistency checks
print("=== Business Rule Violations ===")
print(f"amount_granted > requested_amount * 1.1 : {df.filter(F.col('amount_granted') > F.col('requested_amount') * 1.1).count():,}")
print(f"discretionary_income < 0               : {df.filter(F.col('discretionary_income') < 0).count():,}")
print(f"Booked but no disbursed_at             : {df.filter((F.col('workflow_state') == 'Booked') & F.col('disbursed_at').isNull()).count():,}")
print(f"Approved but not Booked                : {df.filter((F.col('application_status') == 'Approved') & (F.col('workflow_state') != 'Booked')).count():,}")

# Amount reasonableness
df.select(
    F.min("requested_amount"), F.max("requested_amount"),
    F.min("amount_granted"), F.max("amount_granted"),
    F.avg("amount_granted")
).show()

# LTV / Collateral checks (for secured loans)
df.filter(F.col("collateral_type") != "none").select(
    F.avg("loan_to_value_ratio"), 
    F.min("loan_to_value_ratio"), 
    F.max("loan_to_value_ratio")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #  bronze_loan_participations

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col

df = spark.table("bronze_loan_participations")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")

# Distinct participants
distinct_lp = df.select("participation_id").distinct().count()
null_lp     = df.filter(F.col("participation_id").isNull()).count()
print(f"Distinct loan_id : {distinct_lp:,}  |  Nulls: {null_lp:,}")
print(f"Total rows              : {df.count():,}")

# Null Analysis
print("=== Null Analysis ===")
total_rows = df.count()

null_stats = df.select([
    (
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0))
        / F.lit(total_rows) * 100
    ).alias(c)
    for c in df.columns
])

display(null_stats)

# Cardinality Analysis
print("=== Cardinality Analysis ===")
for column_name in [
    "status",
    "loan_type",
    "participant_bank",
    "participant_role"
]:
    print(f"\n{column_name}")
    df.groupBy(column_name).count().orderBy(F.desc("count")).show()


print("=== Profiles ===")
def profile_categorical(df, columns, top_n=10):
    for c in columns:
        print(f"\n--- {c} ---")
        (
            df.groupBy(c)
            .count()
            .orderBy(F.desc("count"))
            .show(top_n, truncate=False)
        )

# Participations
profile_categorical(df, ["participation_direction", "participant_role", "risk_share_type", "status"])

# Cross-Table Relationship & Referential Integrity
df_loans = spark.table("bronze_loans")

print("=== Debit orders referencing loans ===")
# Loan participations vs Loans
part_loans = (
    df.alias("p")
      .join(
          df_loans.select("loan_id").alias("l"),
          on="loan_id",
          how="left"
      )
)

missing_loans = part_loans.filter(F.col("l.loan_id").isNull()).count()

print(f"Participations with missing loan: {missing_loans:,}")

print("=== participation_direction ===")
(df.groupBy("participation_direction")
   .agg(
       F.sum(F.when(F.col("loan_id").isNull(), 1).otherwise(0)).alias("null_loan_id"),
       F.sum(F.when(F.col("external_loan_reference").isNull(), 1).otherwise(0)).alias("null_ext_ref"),
       F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("null_customer_id"),
       F.sum(F.when(F.col("account_id").isNull(), 1).otherwise(0)).alias("null_account_id"),
       F.sum(F.when(F.col("retained_pct").isNull(), 1).otherwise(0)).alias("null_retained_pct"),
       F.count("*").alias("total")
   )
   .show())

(
    df.filter(F.col("participation_direction") == "incoming_participation")
      .join(
          df_loans.select("loan_id"),
          "loan_id",
          "left_anti"
      )
      .count()
)

df.groupBy("participation_direction") \
  .agg(
      F.count("*").alias("rows"),
      F.sum(F.when(F.col("customer_id").isNull(),1).otherwise(0)).alias("null_customer")
  ) \
  .show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_atm_logs

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_atm_logs")

print("=" * 50)
print("ATM LOGS  PROFILING")
print("=" * 50)

# Schema
print("\n=== SCHEMA ===")
df.printSchema()

# Row count
total_rows = df.count()
print(f"\nTotal rows: {total_rows:,}")

# Distincts & nulls
print("\n=== KEY ANALYSIS ===")
for col in df.columns:
    distinct = df.select(col).distinct().count()
    nulls = df.filter(F.col(col).isNull()).count()
    print(f"{col:30} Distinct={distinct:,}  Nulls={nulls:,}")

print("\n=== ORPHAN CHECK===")
df.select("event_type").distinct().show(20, False)
df.select("attempt_result").distinct().show()
df.select("transaction_category").distinct().show(20, False)
df.select("card_type").distinct().show()
df.select("host_response_code").distinct().show()
df.select("currency").distinct().show()

# orphan checks
accounts = spark.table("lh_silver_banking_data.dbo.accounts")
customers = spark.table("lh_silver_banking_data.dbo.customers_individual")

orphan_acct = df.join(accounts.select("account_id"), "account_id", "left_anti").count()
orphan_cust = df.join(customers.select("customer_id"), "customer_id", "left_anti").count()
print("orphan accounts:", orphan_acct)
print("orphan customers:", orphan_cust)

customers_biz = spark.table("lh_silver_banking_data.dbo.customers_business")

# Check if the 2,250 orphans are in business customers instead
orphan_both = (
    df.join(spark.table("lh_silver_banking_data.dbo.customers_individual").select("customer_id"), "customer_id", "left_anti")
      .join(customers_biz.select("customer_id"), "customer_id", "left_anti")
      .select("customer_id").distinct().count()
)
print("orphan in BOTH individual and business:", orphan_both)

orphans = (
    df.join(
        spark.table("lh_silver_banking_data.dbo.customers_individual")
            .select("customer_id"),
        "customer_id",
        "left_anti"
    )
)

orphans.join(
    spark.table("lh_silver_banking_data.dbo.customers_business")
        .select("customer_id"),
    "customer_id",
    "inner"
).count()

print("\n=== CURRENCY ===")
df.groupBy("currency").count().show()


print("\n=== KEY ANALYSIS ===")

df.select("account_number", "masked_card_number", "card_number_hash", "ewallet_recipient_msisdn_entered").show(5, False)


print("\n=== ATM LOCATION ===")
df.groupBy(
    "atm_id",
    "terminal_id",
    "atm_location"
).count().show()

print("\n=== ATTEMPT ANALYSIS ===")
df.groupBy(
    "attempt_result",
    F.col("failure_reason").isNull()
).count().show()

print("\n=== EVENT ANALYSIS ===")
df.groupBy("event_type") \
  .agg(
      F.count(
          F.when(
              F.col("available_balance_returned").isNotNull(),
              1
          )
      ).alias("balance_values")
  ) \
  .show()

print("\n=== EVENT TRANSACTION ANALYSIS ===")
df.filter(F.col("linked_transaction_id").isNotNull()) \
  .select("linked_transaction_id") \
  .distinct() \
  .count()

df.groupBy(
    "event_type",
    "transaction_category"
).count().orderBy("event_type").show(100, False)


df.groupBy(
    F.col("linked_transaction_id").isNull().alias("txn_null"),
    "atm_location"
).count().orderBy("txn_null").show(50, False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_marketing_campaigns_campaigns

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_marketing_campaigns_campaigns")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")


print("=== SAMPLE ROWS ===")
df.show(5, truncate=False)

print("\n=== DISTINCT VALUE DISTRIBUTIONS ===")
for col in ["campaign_type", "channel", "target_segment", "product_focus", "region", "status", "success_metric"]:
    print(f"\n-- {col} --")
    df.groupBy(col).count().orderBy("count", ascending=False).show(truncate=False)

print("\n=== DATE RANGE ===")
df.select(
    F.min("start_date").alias("earliest_start"),
    F.max("start_date").alias("latest_start"),
    F.min("end_date").alias("earliest_end"),
    F.max("end_date").alias("latest_end")
).show()

print("\n=== CAMPAIGN DURATION (days) ===")
df.withColumn("duration_days", F.datediff(F.col("end_date"), F.col("start_date"))) \
  .select(
      F.min("duration_days").alias("min"),
      F.max("duration_days").alias("max"),
      F.avg("duration_days").alias("avg")
  ).show()

print("\n=== BUDGET STATS ===")
df.select(
    F.min("budget_zar").alias("min"),
    F.max("budget_zar").alias("max"),
    F.avg("budget_zar").alias("avg"),
    F.percentile_approx("budget_zar", 0.5).alias("median")
).show()

print("\n=== TARGET CUSTOMERS STATS ===")
df.select(
    F.min("target_customers_count").alias("min"),
    F.max("target_customers_count").alias("max"),
    F.avg("target_customers_count").alias("avg")
).show()

print("\n=== NATURAL KEY CHECK (campaign_id) ===")
total = df.count()
distinct = df.select("campaign_id").distinct().count()
print(f"Total: {total} | Distinct campaign_id: {distinct} | Duplicates: {total - distinct}")

print("\n=== YEAR/MONTH COVERAGE ===")
df.groupBy("year", "month").count().orderBy("year", "month").show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_marketing_campaigns_campaigns")

df.groupBy("campaign_id", "channel", "region") \
  .count() \
  .filter(F.col("count") > 1) \
  .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("bronze_marketing_campaigns_campaigns")

df.groupBy("campaign_id", "channel", "region") \
  .count() \
  .filter(F.col("count") > 1) \
  .join(
      df.select("campaign_id", "channel", "region", "start_date", "end_date", 
                "budget_zar", "target_customers_count", "_source_file"),
      on=["campaign_id", "channel", "region"],
      how="inner"
  ) \
  .orderBy("campaign_id") \
  
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("bronze_marketing_campaigns_campaigns") \
    .groupBy("campaign_type", "status") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## bronze_marketing_campaigns_campaign_responses

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_marketing_campaigns_campaign_responses")

# Schema
print("=== SCHEMA ===")
df.printSchema()

# Row count + distinct on natural key
print(f"\n=== ROW COUNTS ===")
print(f"Total rows     : {df.count():,}")
print("=== SAMPLE ROWS ===")
df.show(5, truncate=False)

print("\n=== DISTINCT VALUE DISTRIBUTIONS ===")
for col in ["response_type", "channel_used"]:
    print(f"\n-- {col} --")
    df.groupBy(col).count().orderBy("count", ascending=False).show(truncate=False)

print("\n=== CONVERSION VALUE STATS (non-null only) ===")
df.filter(F.col("conversion_value_zar").isNotNull()) \
  .select(
      F.min("conversion_value_zar").alias("min"),
      F.max("conversion_value_zar").alias("max"),
      F.avg("conversion_value_zar").alias("avg"),
      F.percentile_approx("conversion_value_zar", 0.5).alias("median"),
      F.count("conversion_value_zar").alias("non_null_count")
  ).show()

print("\n=== ACCOUNT_ID NULL BREAKDOWN BY RESPONSE TYPE ===")
df.groupBy("response_type") \
  .agg(
      F.count("*").alias("total"),
      F.sum(F.when(F.col("account_id").isNull(), 1).otherwise(0)).alias("null_account_id")
  ).orderBy("total", ascending=False).show(truncate=False)

print("\n=== NATURAL KEY CHECK (response_id) ===")
total = df.count()
distinct = df.select("response_id").distinct().count()
print(f"Total: {total} | Distinct response_id: {distinct} | Duplicates: {total - distinct}")

print("\n=== RESPONSES PER CUSTOMER ===")
df.groupBy("customer_id").count() \
  .groupBy("count").count() \
  .orderBy("count", ascending=False).show(20)

print("\n=== RESPONSES PER CAMPAIGN ===")
df.groupBy("campaign_id").count() \
  .orderBy("count", ascending=False).show(20, truncate=False)

print("\n=== DATE RANGE ===")
df.select(
    F.min("response_date").alias("earliest"),
    F.max("response_date").alias("latest")
).show()

print("\n=== YEAR/MONTH COVERAGE ===")
df.groupBy("year", "month").count().orderBy("year", "month").show(50)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("bronze_marketing_campaigns_campaign_responses") \
    .groupBy("response_type") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("bronze_marketing_campaigns_campaign_responses") \
    .groupBy("customer_id") \
    .agg(
        F.max(F.when(F.col("response_type").isin("opted_out", "complained"), 1).otherwise(0)).alias("has_opted_out"),
        F.max(F.when(F.col("response_type").isin("opened", "clicked", "converted"), 1).otherwise(0)).alias("has_engaged")
    ) \
    .withColumn("is_opted_in_marketing",
        (F.col("has_engaged") == 1) & (F.col("has_opted_out") == 0)
    ) \
    .groupBy("is_opted_in_marketing") \
    .count() \
    .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 
