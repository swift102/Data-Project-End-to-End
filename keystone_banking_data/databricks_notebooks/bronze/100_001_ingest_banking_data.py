# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Bronze Ingest — GitHub Banking Dataset
# MAGIC %md
# MAGIC # Bronze Ingest — GitHub Banking Dataset → `keystone_banking.bronze`
# MAGIC
# MAGIC **Source:** `https://github.com/inhamo/Datasets-Advanced-2026` (folder `banking_data`)
# MAGIC
# MAGIC **Target:** Databricks Unity Catalog `keystone_banking.bronze`
# MAGIC
# MAGIC **Strategy:** one-shot shallow clone (the repo is a static snapshot, not a live source), then ingest by format:
# MAGIC
# MAGIC | Format | Files (approx) | Bronze treatment |
# MAGIC |---|---|---|
# MAGIC | Parquet | ~909 | One Delta table per entity, `year`/`month` carried from path |
# MAGIC | CSV | ~592 | One Delta table per sub-folder type, `year`/`month` from path |
# MAGIC | JSONL | ~87 | `transactions` Delta table |
# MAGIC | PDF | ~12,345 | Raw bytes kept in Files + a **manifest** Delta table |
# MAGIC | EML | ~915 | Raw bytes kept in Files + a **manifest** Delta table |
# MAGIC
# MAGIC Unstructured files (PDF/EML) are **not** parsed in Bronze — Bronze stays a faithful raw landing. Text/entity extraction is a Silver concern.
# MAGIC
# MAGIC > **Run-once design.** This re-clones the whole repo each run, which is wasteful for a static dataset. Record the commit SHA (captured in section 3) for thesis reproducibility, then disable the schedule.

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# DBTITLE 1,Load config and setup paths
import json
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StringType
from collections import defaultdict
from functools import reduce
import subprocess, os, shutil, datetime, re, glob
import requests

# Load config from 000_config
config_json = dbutils.notebook.run("../Data-Project-End-to-End/keystone_banking_data/databricks_notebooks/000_config", 60)
config      = json.loads(config_json)

REPO_URL     = config["github_url"]
REPO_SUBDIR  = "banking_data"

# Databricks paths - using Unity Catalog Volumes for Serverless compute
# Volumes provide Spark-accessible storage required for distributed processing
VOLUME_ROOT = "/Volumes/keystone_banking/default/raw_data"
CLONE_DIR   = "/tmp/banking_clone"  # Local staging for git clone
RAW_DIR     = f"{VOLUME_ROOT}/banking_data"  # Spark-accessible location
SPARK_RAW   = RAW_DIR  # Same for Volumes

# Unity Catalog schema for tables
BRONZE_SCHEMA = config.get("bronze_schema", "keystone_banking.bronze")

BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def tbl(name: str) -> str:
    """Qualify table name with Unity Catalog schema."""
    return f"{BRONZE_SCHEMA}.{name}"

print("Batch       :", BATCH_ID)
print("Repo URL    :", REPO_URL)
print("Clone dir   :", CLONE_DIR)
print("Raw dir     :", RAW_DIR)
print("Bronze schema:", BRONZE_SCHEMA)

# COMMAND ----------

# DBTITLE 1,Safeguard - One-time run protection
# MAGIC %md
# MAGIC ## 🛡️ Safeguard: Prevent Accidental Re-runs
# MAGIC
# MAGIC **This is a ONE-TIME migration notebook.** Running it multiple times wastes resources and can corrupt data.
# MAGIC
# MAGIC The cell below checks if Bronze tables already exist. If they do, it will halt execution.

# COMMAND ----------

# DBTITLE 1,Check if already loaded
# ONE-TIME LOAD SAFEGUARD
# This notebook is designed for a single execution to load historical data.
# If you need to re-run, you must explicitly delete the Bronze tables first.

try:
    # Check if any Bronze tables exist
    existing_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").collect() 
                      if r.tableName.startswith("bronze_")]
    
    if existing_tables:
        print("❌ SAFEGUARD TRIGGERED: Bronze tables already exist!")
        print(f"\nFound {len(existing_tables)} existing bronze tables in {BRONZE_SCHEMA}:")
        for t in sorted(existing_tables)[:5]:  # Show first 5
            print(f"  - {t}")
        if len(existing_tables) > 5:
            print(f"  ... and {len(existing_tables) - 5} more")
        print("\n" + "="*70)
        print("This is a ONE-TIME migration notebook.")
        print("\nTo re-run this notebook, you must first:")
        print(f"  1. Manually drop all bronze tables: DROP TABLE {BRONZE_SCHEMA}.bronze_*")
        print(f"  2. Delete the raw files: /dbfs/FileStore/bronze_raw/banking_data/")
        print("  3. Then re-run this notebook.")
        print("="*70)
        
        raise Exception(f"Bronze tables already exist in {BRONZE_SCHEMA}. "
                       "This is a one-time load. Delete existing tables manually to re-run.")
    else:
        print("✅ No existing Bronze tables found. Proceeding with ingestion...")
        
except Exception as e:
    if "bronze_" in str(e).lower() and "already exist" in str(e).lower():
        raise  # Re-raise safeguard exception
    # Schema doesn't exist yet - first run, proceed
    print("✅ Bronze schema is empty or doesn't exist. Proceeding with first-time ingestion...")

# COMMAND ----------

# DBTITLE 1,Shallow clone
# MAGIC %md
# MAGIC ## 2. Shallow clone (single packed transfer — far cheaper than 15k HTTPS GETs)

# COMMAND ----------

# DBTITLE 1,Clone repository
# Clean any prior staging clone so the run is idempotent.
if os.path.exists(CLONE_DIR):
    shutil.rmtree(CLONE_DIR)
os.makedirs(os.path.dirname(CLONE_DIR), exist_ok=True)

# Use verified working URL
CLONE_URL = "https://github.com/inhamo/Datasets-Advanced-2026.git"

print(f"Cloning from: {CLONE_URL}")
print(f"Target dir  : {CLONE_DIR}")

res = subprocess.run(
    ["git", "clone", "--depth", "1", CLONE_URL, CLONE_DIR],
    capture_output=True, text=True
)
print(res.stdout)
if res.stderr:
    print("STDERR:", res.stderr)
res.check_returncode()
print("✅ Clone successful!")

# COMMAND ----------

# DBTITLE 1,Capture provenance
# MAGIC %md
# MAGIC ## 3. Capture provenance (commit SHA) and check size before going further

# COMMAND ----------

# DBTITLE 1,Get commit SHA and size
commit_sha = subprocess.run(
    ["git", "-C", CLONE_DIR, "rev-parse", "HEAD"],
    capture_output=True, text=True
).stdout.strip()

size_human = subprocess.run(
    ["du", "-sh", CLONE_DIR], capture_output=True, text=True
).stdout.strip()

print("Cloned commit:", commit_sha)
print("Clone size   :", size_human)
print("\nIf size is multiple GB, confirm notebook disk headroom before the copytree in the next cell.")

# COMMAND ----------

# DBTITLE 1,Persist raw bytes
# MAGIC %md
# MAGIC ## 4. Persist raw bytes into DBFS
# MAGIC
# MAGIC The clone path is staging; copy `banking_data` into a stable `bronze_raw` location so the PDF/EML bytes survive beyond this session and the manifests point at durable paths.

# COMMAND ----------

# DBTITLE 1,Copy to bronze_raw
src = os.path.join(CLONE_DIR, REPO_SUBDIR)
assert os.path.isdir(src), f"Expected {src} to exist after clone"

print(f"Copying from {src} to {RAW_DIR}...")
print("This may take 2-3 minutes for 1.1GB of data...")

# Copy from local /tmp to UC Volume using Python shutil
# Volumes support regular Python file operations
os.makedirs(os.path.dirname(RAW_DIR), exist_ok=True)
shutil.copytree(src, RAW_DIR, dirs_exist_ok=True)

print(f"\n✅ Copied to Volume successfully!")

# Free the staging clone (incl. .git history) now that raw bytes are landed in Volume.
shutil.rmtree(CLONE_DIR, ignore_errors=True)
print("Raw landing ready at:", RAW_DIR)

# COMMAND ----------

# DBTITLE 1,Helpers
# MAGIC %md
# MAGIC ## 5. Helpers — Bronze metadata + path-derived partition columns
# MAGIC
# MAGIC Per the Bronze rules: never modify source columns; add ingestion metadata only. The `year`/`month` come from the **folder path**, not the filename — critical for the sub-folder CSVs which carry no date in their name.

# COMMAND ----------

# DBTITLE 1,Bronze helper functions
def with_bronze_meta(df):
    """Add ingestion metadata + path-derived partition cols. Source columns untouched."""
    if "_source_file" not in df.columns:
        df = df.withColumn("_source_file", F.col("_metadata.file_path"))
    return (df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_batch_id",         F.lit(BATCH_ID))
        .withColumn("_commit_sha",       F.lit(commit_sha))
        .withColumn("year",  F.regexp_extract(F.col("_source_file"), r"/(\d{4})/", 1))
        .withColumn("month", F.regexp_extract(F.col("_source_file"), r"/\d{4}/(\d{2})/", 1))
    )


def write_bronze(df, name, partition_cols=("year", "month")):
    """Full-reload overwrite — acceptable for a static one-shot snapshot.
    overwriteSchema handles per-month schema drift across entities."""
    table_name = tbl(name)
    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .saveAsTable(table_name))
    print(f"  ✅ wrote {table_name}")


def read_reconciled(spark_paths):
    """Read each parquet file separately, cast type-conflicting columns to STRING, then union.
    Returns (df, conflict_cols)."""
    dfs = []
    for p in spark_paths:
        try:
            df = spark.read.parquet(p).withColumn(
                "_source_file", F.col("_metadata.file_path")
            )
            # Skip empty schema files (no columns = empty parquet shell)
            real_cols = [c for c in df.columns if c != "_source_file"]
            if not real_cols:
                print(f"  ⚠️  Empty schema (0 columns), skipping: {os.path.basename(p)}")
                continue
            df.limit(1).count()
            dfs.append(df)
        except Exception as e:
            print(f"  ⚠️  Unreadable, skipping: {os.path.basename(p)} — {str(e)[:80]}")
            continue

    if not dfs:
        raise ValueError("No readable files found in this entity batch")

    types = defaultdict(set)
    for d in dfs:
        for field in d.schema.fields:
            types[field.name].add(field.dataType.simpleString())

    conflict_cols = {c for c, ts in types.items() if len(ts) > 1}

    if conflict_cols:
        dfs = [
            d.select(*[
                F.col(c).cast(StringType()).alias(c) if c in conflict_cols
                else F.col(c)
                for c in d.columns
            ])
            for d in dfs
        ]

    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    return df, conflict_cols

# COMMAND ----------

# DBTITLE 1,Parquet ingestion
# MAGIC %md
# MAGIC ## 6. Parquet — one Delta table per entity
# MAGIC
# MAGIC Files are named `<entity>_<year>_<month>.parquet`. We group by entity prefix so each becomes its own Bronze table rather than collapsing unrelated schemas into one frame.

# COMMAND ----------

# DBTITLE 1,Ingest Parquet files
# Discover all parquet entities from the raw landing
parquet_paths = glob.glob(f"{RAW_DIR}/*/*/*.parquet")
entities = sorted({
    re.sub(r"_\d{4}_\d{2}\.parquet$", "", os.path.basename(p))
    for p in parquet_paths
})

print(f"Parquet entities discovered ({len(entities)}):")
for e in entities:
    print(" ", e)

# Ingest each entity into its own Delta table
for entity in entities:
    paths = glob.glob(f"{RAW_DIR}/*/*/{entity}_*.parquet")
    # Paths in /tmp are already in correct format for Spark
    spark_paths = paths

    if not spark_paths:
        print(f"  WARNING: no files found for entity '{entity}' — skipping")
        continue

    df, conflicts = read_reconciled(spark_paths)

    if conflicts:
        print(f"  {entity}: reconciled columns {sorted(conflicts)}")

    df = with_bronze_meta(df)
    write_bronze(df, f"bronze_{entity}")

# COMMAND ----------

# DBTITLE 1,JSONL transactions
# MAGIC %md
# MAGIC ## 7. JSONL — monthly `transactions.jsonl`
# MAGIC
# MAGIC These live at `<year>/<month>/transactions.jsonl`. Spark reads JSONL natively with `spark.read.json`.
# MAGIC
# MAGIC **Note:** If files are Git LFS pointers, they need to be hydrated first (download actual content from GitHub).

# COMMAND ----------

# DBTITLE 1,Hydrate LFS pointers and ingest JSONL
def is_lfs_pointer(path, sniff=120):
    try:
        with open(path, "rb") as f:
            return f.read(sniff).startswith(b"version https://git-lfs.github.com/spec/v1")
    except Exception:
        return False

# Note: Git LFS files were downloaded during shallow clone, so no hydration needed!
# The actual structure is year/month/day/transactions.jsonl

# Ingest all transactions.jsonl into one partitioned Delta table
jsonl_paths = glob.glob(f"{RAW_DIR}/*/*/*/transactions.jsonl")  # year/month/day/file
spark_jsonl = jsonl_paths

print(f"Found {len(spark_jsonl)} transactions.jsonl files")

df_txn = (spark.read
          .option("mode", "PERMISSIVE")
          .json(spark_jsonl))
# Unity Catalog requires _metadata.file_path instead of input_file_name()
df_txn = df_txn.withColumn("_source_file", F.col("_metadata.file_path"))
df_txn = with_bronze_meta(df_txn)
write_bronze(df_txn, "bronze_transactions")
print(f"Transactions rows: {df_txn.count():,}")

# COMMAND ----------

# DBTITLE 1,CSV ingestion
# MAGIC %md
# MAGIC ## 8. CSV — one Delta table per sub-folder type
# MAGIC
# MAGIC CSVs sit in typed sub-folders (`customer_communications/`, `marketing_campaigns/`, `collections_cases/`). **Their filenames carry no date** — the period lives only in the folder path, which `with_bronze_meta` extracts into `year`/`month`. We auto-discover the folder/file combinations rather than hard-coding them, so new folder types in later months are picked up.

# COMMAND ----------

# DBTITLE 1,Ingest CSV files
# Discover CSV groups from disk
csv_paths  = glob.glob(f"{RAW_DIR}/*/*/*/*.csv")   # year/month/<subfolder>/<file>.csv
csv_groups = {}
for p in csv_paths:
    parts      = p.split(os.sep)
    sub_folder = parts[-2]
    file_stem  = os.path.splitext(parts[-1])[0]
    csv_groups.setdefault((sub_folder, parts[-1]), file_stem)

print("CSV groups discovered:")
for (sub, fname), stem in sorted(csv_groups.items()):
    print(f"  {sub}/{fname}  ->  bronze_{sub}_{stem}")

# Ingest each CSV group into its own Delta table
for (sub_folder, fname), stem in csv_groups.items():
    pattern = f"{SPARK_RAW}/*/*/{sub_folder}/{fname}"
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")   # Bronze tolerance; Silver will enforce explicit schema
          .option("mode", "PERMISSIVE")
          .csv(pattern))
    df = with_bronze_meta(df)
    write_bronze(df, f"bronze_{sub_folder}_{stem}")

# COMMAND ----------

# DBTITLE 1,Unstructured files
# MAGIC %md
# MAGIC ## 9. Unstructured — PDF & EML manifests (bytes stay on disk)
# MAGIC
# MAGIC Bronze does **not** parse these. We build a queryable catalog (one row per file) using Spark's `binaryFile` source, which reads path + metadata without interpreting content. The actual bytes remain under `RAW_DIR` for Silver-layer extraction to consume.

# COMMAND ----------

# DBTITLE 1,Build file manifests
def build_manifest(glob_filter: str, file_type: str, table: str):
    df = (spark.read.format("binaryFile")
          .option("recursiveFileLookup", "true")
          .option("pathGlobFilter", glob_filter)
          .load(SPARK_RAW)
          .select("path", "length", "modificationTime"))
    df = (df
          .withColumn("file_name",         F.element_at(F.split(F.col("path"), "/"), -1))
          .withColumn("year",              F.regexp_extract("path", r"/(\d{4})/", 1))
          .withColumn("month",             F.regexp_extract("path", r"/\d{4}/(\d{2})/", 1))
          .withColumn("file_type",         F.lit(file_type))
          .withColumn("_batch_id",         F.lit(BATCH_ID))
          .withColumn("_commit_sha",       F.lit(commit_sha))
          .withColumn("_ingest_timestamp", F.current_timestamp()))
    write_bronze(df, table)
    return df.count()

n_pdf = build_manifest("*.pdf", "pdf", "bronze_pdf_manifest")
n_eml = build_manifest("*.eml", "eml", "bronze_eml_manifest")
print(f"PDF manifest rows : {n_pdf:,}")
print(f"EML manifest rows : {n_eml:,}")

# COMMAND ----------

# DBTITLE 1,Account counter
# MAGIC %md
# MAGIC ## 10. Account counter JSON (single small control file)

# COMMAND ----------

# DBTITLE 1,Ingest account counter
acct = (spark.read
         .option("multiline", "true")
         .json(f"{SPARK_RAW}/account_counter.json"))

acct = (acct
         .withColumn("_batch_id",         F.lit(BATCH_ID))
         .withColumn("_commit_sha",       F.lit(commit_sha))
         .withColumn("_ingest_timestamp", F.current_timestamp()))

(acct.write.format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true")
     .saveAsTable(tbl("bronze_account_counter")))

print("✅ wrote", tbl("bronze_account_counter"))

# COMMAND ----------

# DBTITLE 1,Ingest summary
# MAGIC %md
# MAGIC ## 11. Ingest summary

# COMMAND ----------

# DBTITLE 1,Display ingestion stats
# List all bronze tables in the schema
tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").collect() 
          if r.tableName.startswith("bronze_")]

print(f"Commit ingested      : {commit_sha}")
print(f"Batch                : {BATCH_ID}")
print(f"Bronze tables created: {len(tables)}\n")

for t in sorted(tables):
    c = spark.table(f"{BRONZE_SCHEMA}.{t}").count()
    print(f"  {t:55s} {c:>12,} rows")

# COMMAND ----------

# DBTITLE 1,Validation
# MAGIC %md
# MAGIC ## 12. Validation — Check expected tables
# MAGIC
# MAGIC Verify that all expected tables were created and are non-empty.

# COMMAND ----------

# DBTITLE 1,Validate expected tables
# Tables actually present in the inhamo/Datasets-Advanced-2026 repository
# (loan_participations and rejected_applications do not exist in this dataset version)
EXPECTED_TABLES = [
    "bronze_accounts",
    "bronze_customers",
    "bronze_atm_logs",
    "bronze_account_limits_history",
    "bronze_debit_orders",
    "bronze_loans",
    "bronze_transactions",
    "bronze_account_counter",
    "bronze_pdf_manifest",
    "bronze_eml_manifest",
]

landed = {r.tableName for r in spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").collect()}
print("=" * 60)
for expected in EXPECTED_TABLES:
    status = "✅" if expected in landed else "❌ MISSING"
    print(f"  {status}  {expected}")
print("=" * 60)

# Flag any expected table that landed empty
for expected in EXPECTED_TABLES:
    if expected in landed:
        cnt = spark.table(f"{BRONZE_SCHEMA}.{expected}").count()
        if cnt == 0:
            print(f"  ⚠️  {expected} landed but has 0 rows")

# COMMAND ----------

# DBTITLE 1,Alternative: Auto Loader
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # 🚀 Alternative Approach: Auto Loader (Production-Grade)
# MAGIC
# MAGIC **The cells above use shallow clone** - simple and acceptable for one-time thesis data migration.
# MAGIC
# MAGIC **For production or incremental updates,** use **Databricks Auto Loader** instead:
# MAGIC
# MAGIC ## Why Auto Loader?
# MAGIC
# MAGIC | Feature | Shallow Clone (Current) | Auto Loader (Production) |
# MAGIC |---------|------------------------|-------------------------|
# MAGIC | **Incremental** | ❌ Re-downloads everything | ✅ Only new/changed files |
# MAGIC | **Cost** | ❌ High (full reload) | ✅ Low (incremental) |
# MAGIC | **Speed** | ❌ 15-30 min (full) | ✅ Seconds-minutes (incremental) |
# MAGIC | **Resume** | ❌ Cannot resume | ✅ Checkpoint-based recovery |
# MAGIC | **Schema drift** | ⚠️ Manual reconciliation | ✅ Automatic handling |
# MAGIC | **State tracking** | ❌ None | ✅ Automatic (checkpoints) |
# MAGIC | **Scalability** | ⚠️ Limited | ✅ Millions of files |
# MAGIC | **Use case** | One-time migration | Production pipelines |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Auto Loader Implementation Examples
# MAGIC
# MAGIC The cells below show how to implement Auto Loader for each data type. **Do NOT run these cells** - they're for reference only.

# COMMAND ----------

# DBTITLE 1,Auto Loader: Parquet
# MAGIC %md
# MAGIC ### Auto Loader: Parquet Entities
# MAGIC
# MAGIC **Use when:** Files land continuously in `/FileStore/landing/banking_data/`

# COMMAND ----------

# DBTITLE 1,Auto Loader Parquet code
# REFERENCE ONLY - Do not run (this is for production incremental pipelines)

# Auto Loader for Parquet - reads only new files since last checkpoint
# Handles schema evolution automatically

# Example: Ingest accounts parquet files
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/FileStore/checkpoints/bronze_accounts/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema changes
    .load("/FileStore/landing/banking_data/*/*/accounts_*.parquet")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("year", F.regexp_extract(F.col("_source_file"), r"/(\d{4})/", 1))
    .withColumn("month", F.regexp_extract(F.col("_source_file"), r"/\d{4}/(\d{2})/", 1))
    .writeStream
    .format("delta")
    .outputMode("append")  # Only new records
    .option("checkpointLocation", "/FileStore/checkpoints/bronze_accounts/state")
    .partitionBy("year", "month")
    .trigger(once=True)  # Or use .trigger(availableNow=True) for continuous
    .table("keystone_banking.bronze.bronze_accounts")
)

print("✅ Auto Loader processed new accounts files only")

# COMMAND ----------

# DBTITLE 1,Auto Loader: JSONL
# MAGIC %md
# MAGIC ### Auto Loader: JSONL Transactions

# COMMAND ----------

# DBTITLE 1,Auto Loader JSONL code
# REFERENCE ONLY - Do not run

# Auto Loader for JSONL - handles multi-line and schema inference
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")  # JSONL is read as 'json'
    .option("cloudFiles.schemaLocation", "/FileStore/checkpoints/bronze_transactions/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("mode", "PERMISSIVE")  # Handle malformed records
    .load("/FileStore/landing/banking_data/*/*/transactions.jsonl")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("year", F.regexp_extract(F.col("_source_file"), r"/(\d{4})/", 1))
    .withColumn("month", F.regexp_extract(F.col("_source_file"), r"/\d{4}/(\d{2})/", 1))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/FileStore/checkpoints/bronze_transactions/state")
    .partitionBy("year", "month")
    .trigger(once=True)
    .table("keystone_banking.bronze.bronze_transactions")
)

print("✅ Auto Loader processed new transaction files only")

# COMMAND ----------

# DBTITLE 1,Auto Loader: CSV
# MAGIC %md
# MAGIC ### Auto Loader: CSV Files

# COMMAND ----------

# DBTITLE 1,Auto Loader CSV code
# REFERENCE ONLY - Do not run

# Auto Loader for CSV - handles headers and schema inference
# Example: customer communications
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/FileStore/checkpoints/bronze_customer_communications/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .load("/FileStore/landing/banking_data/*/*/customer_communications/*.csv")
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("year", F.regexp_extract(F.col("_source_file"), r"/(\d{4})/", 1))
    .withColumn("month", F.regexp_extract(F.col("_source_file"), r"/\d{4}/(\d{2})/", 1))
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/FileStore/checkpoints/bronze_customer_communications/state")
    .partitionBy("year", "month")
    .trigger(once=True)
    .table("keystone_banking.bronze.bronze_customer_communications")
)

print("✅ Auto Loader processed new CSV files only")

# COMMAND ----------

# DBTITLE 1,Auto Loader: Binary files
# MAGIC %md
# MAGIC ### Auto Loader: Binary Files (PDF/EML Manifests)

# COMMAND ----------

# DBTITLE 1,Auto Loader binary code
# REFERENCE ONLY - Do not run

# Auto Loader for binary files - builds manifest incrementally
(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", "/FileStore/checkpoints/bronze_pdf_manifest/schema")
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load("/FileStore/landing/banking_data/")
    .select("path", "length", "modificationTime")
    .withColumn("file_name", F.element_at(F.split(F.col("path"), "/"), -1))
    .withColumn("year", F.regexp_extract("path", r"/(\d{4})/", 1))
    .withColumn("month", F.regexp_extract("path", r"/\d{4}/(\d{2})/", 1))
    .withColumn("file_type", F.lit("pdf"))
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/FileStore/checkpoints/bronze_pdf_manifest/state")
    .partitionBy("year", "month")
    .trigger(once=True)
    .table("keystone_banking.bronze.bronze_pdf_manifest")
)

print("✅ Auto Loader processed new PDF files only")

# COMMAND ----------

# DBTITLE 1,Auto Loader benefits summary
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Auto Loader Benefits Summary
# MAGIC
# MAGIC ### Cost Savings
# MAGIC * **First run:** Similar time/cost to shallow clone
# MAGIC * **Subsequent runs:** Only processes NEW/CHANGED files
# MAGIC * **Example:** If 10 new files arrive, only those 10 are processed (not all 15,000)
# MAGIC
# MAGIC ### Operational Benefits
# MAGIC * ✅ **Exactly-once processing** - no duplicates
# MAGIC * ✅ **Automatic schema evolution** - handles new columns gracefully
# MAGIC * ✅ **Checkpoint recovery** - resumes from last processed file if interrupted
# MAGIC * ✅ **Scalability** - handles millions of files efficiently
# MAGIC * ✅ **Cloud-native** - works with S3, ADLS, GCS
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC | Scenario | Recommended Approach |
# MAGIC |----------|---------------------|
# MAGIC | **One-time thesis data migration** | 👍 Shallow Clone (current cells above) |
# MAGIC | **Daily incremental loads** | 🚀 Auto Loader |
# MAGIC | **Continuous streaming** | 🚀 Auto Loader with `.trigger(processingTime='1 minute')` |
# MAGIC | **Production pipelines** | 🚀 Auto Loader |
# MAGIC | **Ad-hoc exploration** | 👍 Shallow Clone or direct reads |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📖 For Our Thesis
# MAGIC
# MAGIC **You should use the shallow clone approach (cells 1-12 above)** because:
# MAGIC * One-time historical data load (2019-2024)
# MAGIC * Simple and straightforward
# MAGIC * Easier to document and explain in thesis
# MAGIC * No need for ongoing updates
# MAGIC
# MAGIC **Document Auto Loader** in your thesis as:
# MAGIC * "Future production enhancement"
# MAGIC * "Recommended for operational deployment"
# MAGIC * "Not implemented due to one-time migration scope"
# MAGIC
# MAGIC This demonstrates awareness of production best practices without over-engineering the thesis implementation. ✅