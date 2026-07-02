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

# ## Imports + Config

# CELL ********************

import json, datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import *

config = json.loads(
    notebookutils.notebook.run(
        "000_Config",
        90,
        {"useRootDefaultLakehouse": True}
    )
)

MASK_SALT = config["MASK_SALT"]


SILVER_BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

SOURCE_TABLE = "lh_bronze_banking_data_modern_data.dbo.bronze_atm_logs"

TARGET_TABLE = "atm_logs"

TARGET_DQ = "silver_dq_atm_logs"


PIPELINE_NAME = "200_015_transform_atm_logs_silver"

WATERMARK_COL = "_ingest_timestamp"


print("Batch:", SILVER_BATCH_ID)
print("Source:", SOURCE_TABLE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Watermark

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

# ## Read Watermark

# CELL ********************

watermark_df = spark.sql(f"""
    SELECT watermark_value
    FROM control.batch_watermark
    WHERE pipeline_name = '{PIPELINE_NAME}'
    ORDER BY watermark_value DESC
    LIMIT 1
""")

if watermark_df.count() == 0:
    last_watermark = None
else:
    last_watermark = watermark_df.collect()[0][0]

print("Last watermark:", last_watermark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Bronze Incrementally

# CELL ********************

bronze_raw = spark.table(SOURCE_TABLE)

if last_watermark is None:
    bronze = bronze_raw
else:
    bronze = bronze_raw.filter(F.col("_ingest_timestamp") > last_watermark)

print(f"Rows in this batch: {bronze.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dedup

# CELL ********************

# Deduplicate on atm_log_id before any transforms.
# Keep the latest ingest record per log ID in case the same event
# was re-ingested across batches.
w = Window.partitionBy("atm_log_id").orderBy(F.col("_ingest_timestamp").desc())

deduped = (
    bronze
    .withColumn("_row_rank", F.row_number().over(w))
    .filter(F.col("_row_rank") == 1)
    .drop("_row_rank")
)

print(f"Before dedup : {bronze.count():,}")
print(f"After dedup  : {deduped.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Schema Enforcement

# CELL ********************

silver = deduped.select(

    # Surrogate / control keys 
    F.col("atm_log_id").cast(StringType()),

    # Timestamps 
    F.to_timestamp("event_timestamp").alias("event_timestamp"),
    F.to_date("event_date").alias("event_date"),
    F.col("event_time").cast(StringType()),          # HH:mm:ss string; no native time type in Spark

    # ATM and Terminal identity 
    F.trim(F.col("atm_id")).alias("atm_id"),
    F.trim(F.col("terminal_id")).alias("terminal_id"),
    F.trim(F.col("atm_location")).alias("atm_location"),
    F.trim(F.col("atm_province")).alias("atm_province"),
    F.col("atm_latitude").cast(DoubleType()),
    F.col("atm_longitude").cast(DoubleType()),

    # Customer and Account reference keys 
    F.col("customer_id").cast(StringType()),
    F.col("account_id").cast(StringType()),
    F.col("account_number").cast(StringType()),
    F.trim(F.lower(F.col("account_status"))).alias("account_status"),

    # Card details 
    F.col("masked_card_number").cast(StringType()),  # already masked in Bronze (XXXX format); pass through
    F.col("card_number_hash").cast(StringType()),    # pre-hashed in Bronze source
    F.trim(F.lower(F.col("card_type"))).alias("card_type"),
    F.col("card_expiry_date").cast(StringType()),
    F.col("card_block_status").cast(BooleanType()),

    # Event classification 
    F.trim(F.lower(F.col("event_type"))).alias("event_type"),
    F.trim(F.lower(F.col("attempt_result"))).alias("attempt_result"),
    F.trim(F.lower(F.col("failure_reason"))).alias("failure_reason"),

    # Transaction amounts 
    F.col("amount").cast(DoubleType()),
    F.trim(F.upper(F.col("currency"))).alias("currency"),

    # Balance enquiry
    F.col("balance_enquiry_requested").cast(BooleanType()),
    F.col("available_balance_returned").cast(DoubleType()),

    # PIN / Hardware state
    F.col("pin_attempt_number").cast(StringType()),  # ~172 / 169K non-null; valid sparse field
    F.trim(F.lower(F.col("cash_bin_status"))).alias("cash_bin_status"),
    F.col("receipt_printed").cast(BooleanType()),

    # Network / Host 
    F.col("host_response_code").cast(StringType()),
    F.col("network_latency_ms").cast(DoubleType()),

    # Transaction linkage 
    F.col("linked_transaction_id").cast(StringType()),
    F.trim(F.lower(F.col("transaction_category"))).alias("transaction_category"),
    F.trim(F.lower(F.col("transaction_status"))).alias("transaction_status"),

    # Source system 
    F.trim(F.lower(F.col("source_system"))).alias("source_system"),

    # eWallet fields 
    F.col("ewallet_reference").cast(StringType()),
    F.col("ewallet_recipient_msisdn_entered").cast(StringType()),  # raw PII — masked in next cell
    F.trim(F.lower(F.col("ewallet_error_type"))).alias("ewallet_error_type"),

    # Ingest metadata
    F.col("_source_file"),
    F.col("_ingest_timestamp"),
    F.col("_batch_id"),
    F.col("_commit_sha"),
    F.col("year"),
    F.col("month"),
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## PII Masking

# CELL ********************

def mask_pii(col_name: str) -> F.Column:
    """SHA-256 deterministic mask with project salt. Nulls pass through as NULL."""
    return F.when(
        F.col(col_name).isNotNull(),
        F.sha2(F.concat(F.lit(MASK_SALT), F.col(col_name)), 256)
    ).otherwise(F.lit(None).cast(StringType()))

# ewallet_recipient_msisdn_entered is a customer-entered phone number — mask before Silver write.
silver = (
    silver
    .withColumn("ewallet_recipient_msisdn_masked", mask_pii("ewallet_recipient_msisdn_entered"))
    .drop("ewallet_recipient_msisdn_entered")   # drop raw PII; masked column replaces it
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ISO 8583 Host Response Code Decoding

# CELL ********************

silver = silver.withColumn(
    "host_response_code_label",
    F.when(F.col("host_response_code") == "00", "approved")
     .when(F.col("host_response_code") == "05", "do_not_honour")
     .when(F.col("host_response_code") == "51", "insufficient_funds")
     .when(F.col("host_response_code") == "55", "incorrect_pin")
     .when(F.col("host_response_code") == "57", "transaction_not_permitted")
     .when(F.col("host_response_code") == "61", "exceeds_withdrawal_limit")
     .when(F.col("host_response_code") == "68", "response_received_too_late")
     .when(F.col("host_response_code") == "91", "issuer_unavailable")
     .otherwise("unknown")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# CELL: Conditional Null Fills
#
# transaction_category and linked_transaction_id are structurally NULL for
# non-transactional event types. These are not data quality gaps — they are
# expected by design. Fill with 'not_applicable' only for those event types
# so that genuine NULLs on cash_withdrawal rows remain detectable.
#
# Known null event_types from profiling:
#   balance_enquiry, card_retained, card_status_check, cardless_withdrawal,
#   ewallet_cashout, ewallet_send_voucher, mini_statement, pin_change
#
# Known anomaly: all linked_transaction_id NULLs (13,555 rows) correlate
# with named city ATM locations (not 'ATM network terminal'). This is a
# data generation artefact — do not attempt to fix, document only.
# ─────────────────────────────────────────────────────────────────────────────

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


NON_TRANSACTIONAL_EVENTS = [
    "balance_enquiry",
    "card_retained",
    "card_status_check",
    "cardless_withdrawal",
    "ewallet_cashout",
    "ewallet_send_voucher",
    "mini_statement",
    "pin_change",
]

silver = (
    silver
    .withColumn(
        "transaction_category",
        F.when(
            F.col("event_type").isin(NON_TRANSACTIONAL_EVENTS) & F.col("transaction_category").isNull(),
            F.lit("not_applicable")
        ).otherwise(F.col("transaction_category"))
    )
    .withColumn(
        "linked_transaction_id",
        F.when(
            F.col("event_type").isin(NON_TRANSACTIONAL_EVENTS) & F.col("linked_transaction_id").isNull(),
            F.lit("not_applicable")
        ).otherwise(F.col("linked_transaction_id"))
    )
    .withColumn(
        "transaction_status",
        F.when(
            F.col("event_type").isin(NON_TRANSACTIONAL_EVENTS) & F.col("transaction_status").isNull(),
            F.lit("not_applicable")
        ).otherwise(F.col("transaction_status"))
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Derived Business Fields

# CELL ********************

# Derived after null fills so linked_transaction_id reflects the filled value.
# is_financial_transaction keyed on event_type (not the filled string) for clarity.
silver = (
    silver
    .withColumn(
        "is_financial_transaction",
        ~F.col("event_type").isin(NON_TRANSACTIONAL_EVENTS)
    )
    .withColumn(
        "atm_event_group",
        F.when(
            ~F.col("event_type").isin(NON_TRANSACTIONAL_EVENTS),
            "financial_transaction"
        ).otherwise("atm_service_event")
    )
    .withColumn(
        "is_failed_attempt",
        F.col("attempt_result") == "failed"
    )
    .withColumn(
        "is_generic_location",
        F.col("atm_location") == "ATM network terminal"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DQ Flags

# CELL ********************

# Load both Silver customer tables for orphan check.
# 2,250 ATM log customer_ids not in customers_individual;
# 9 of those resolve to customers_business; 2,241 are true orphans.
# Rows are flagged rather than dropped — ATM events are operational records.
all_known_customers = (
    spark.table("lh_silver_banking_data.dbo.customers_individual").select("customer_id")
    .union(
        spark.table("lh_silver_banking_data.dbo.customers_business").select("customer_id")
    )
    .distinct()
    .withColumn("_known_customer", F.lit(True))
)

silver = (
    silver
    .join(all_known_customers, "customer_id", "left")
    .withColumn("dq_orphan_customer",      F.col("_known_customer").isNull())
    .withColumn("dq_non_zar_currency",     F.col("currency") != "ZAR")
    .withColumn(
        "dq_missing_failure_reason",
        (F.col("attempt_result") == "failed") & F.col("failure_reason").isNull()
    )
    .drop("_known_customer")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## DQ Checks

# CELL ********************

REQUIRED = [
    "atm_log_id", "atm_id", "terminal_id",
    "customer_id", "account_id",
    "event_timestamp", "event_type",
]

print("=" * 60)
print("REQUIRED FIELD CHECK")
print("=" * 60)
for col in REQUIRED:
    n = silver.filter(F.col(col).isNull()).count()
    print("\u2705" if n == 0 else "\u274c", col, n)

print()
print("=" * 60)
print("ATM BUSINESS RULES")
print("=" * 60)

failed_missing_reason = (
    silver
    .filter((F.col("attempt_result") == "failed") & F.col("failure_reason").isNull())
    .count()
)
print("Failed without failure_reason :", failed_missing_reason)

balance_missing = (
    silver
    .filter(
        (F.col("event_type") == "balance_enquiry") &
        F.col("available_balance_returned").isNull()
    )
    .count()
)
print("Balance enquiry without balance :", balance_missing)

orphan_count = silver.filter(F.col("dq_orphan_customer") == True).count()
print("Orphan customers flagged        :", orphan_count)

non_zar_count = silver.filter(F.col("dq_non_zar_currency") == True).count()
print("Non-ZAR currency rows flagged   :", non_zar_count)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add Silver audit columns

# CELL ********************

silver = (
    silver
    .withColumn("silver_batch_id",        F.lit(SILVER_BATCH_ID))
    .withColumn("created_timestamp",      F.current_timestamp())
    .withColumn("updated_timestamp",      F.current_timestamp())
)

print(f"Silver rows ready to merge: {silver.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  Write to Silver Lakehouse

# CELL ********************

def merge_silver(df, table_name, business_key):

    # First load — table does not exist yet
    if not spark.catalog.tableExists(table_name):
        (
            df.write
              .format("delta")
              .mode("overwrite")
              .saveAsTable(table_name)
        )
        inserts = df.count()
        updates = 0
        print(f"\u2705 Created {table_name}")
        print(f"Inserts : {inserts:,}")
        print(f"Updates : {updates:,}")
        return inserts, updates

    # Subsequent loads — merge into existing table
    existing_keys = spark.table(table_name).select(business_key)

    inserts = df.join(existing_keys, business_key, "left_anti").count()
    updates = df.count() - inserts

    target = DeltaTable.forName(spark, table_name)

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

    print(f"\u2705 Merged {table_name}")
    print(f"Inserts : {inserts:,}")
    print(f"Updates : {updates:,}")
    return inserts, updates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rows_inserted, rows_updated = merge_silver(
    silver,
    TARGET_TABLE,
    "atm_log_id"
)

rows_written = silver.count()

print(f"Rows Processed : {rows_written:,}")
print(f"Rows Inserted  : {rows_inserted:,}")
print(f"Rows Updated   : {rows_updated:,}")


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
    "pipeline_name":      PIPELINE_NAME,
    "source_table":       SOURCE_TABLE,
    "watermark_column":   WATERMARK_COL,
    "watermark_value":    new_watermark,
    "batch_id":           SILVER_BATCH_ID,
    "rows_processed":     bronze.count(),
    "rows_inserted": rows_inserted,
    "rows_updated": rows_updated,
    "status":             "SUCCESS",
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
