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

# # Bronze Ingest — GitHub Banking Dataset → `lh_bronze_banking_data`
# 
# **Source:** `https://github.com/inhamo/Datasets-Advanced-2026` (folder `banking_data`)
# 
# **Target:** Microsoft Fabric Lakehouse `lh_bronze_banking_data`
# 
# **Strategy:** one-shot shallow clone (the repo is a static snapshot, not a live source), then ingest by format:
# 
# | Format | Files (approx) | Bronze treatment |
# |---|---|---|
# | Parquet | ~909 | One Delta table per entity, `year`/`month` carried from path |
# | CSV | ~592 | One Delta table per sub-folder type, `year`/`month` from path |
# | JSONL | ~87 | `transactions` Delta table |
# | PDF | ~12,345 | Raw bytes kept in Files + a **manifest** Delta table |
# | EML | ~915 | Raw bytes kept in Files + a **manifest** Delta table |
# 
# Unstructured files (PDF/EML) are **not** parsed in Bronze — Bronze stays a faithful raw landing. Text/entity extraction is a Silver concern.
# 
# > **Run-once design.** This re-clones the whole repo each run, which is wasteful for a static dataset. Record the commit SHA (captured in cell 3) for thesis reproducibility, then disable the schedule.


# MARKDOWN ********************

# 1. Can incremental loading happening after cloning?
# 2. Can you apply or implement watermarking using the cloning method?
# 3. Validating metadata of the data -:
# 4. Is the pipeline idempotent?
# 5. 


# MARKDOWN ********************

# ## 1. Configuration

# CELL ********************

import json
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StringType
from collections import defaultdict
from functools import reduce
import subprocess, os, shutil, datetime, re, glob
import requests

# Load config from 000_Config
config_json = mssparkutils.notebook.run("000_Config", 60, {"useRootDefaultLakehouse": True})
config      = json.loads(config_json)

REPO_URL     = config["github_url"]
REPO_SUBDIR  = "banking_data"

# Lakehouse paths
LH_ROOT   = "/lakehouse/default/Files"
CLONE_DIR = f"{LH_ROOT}/_staging/banking_clone"
RAW_DIR   = f"{LH_ROOT}/bronze_raw/banking_data"
SPARK_RAW = "Files/bronze_raw/banking_data"

# Table prefix (empty = no schema prefix; set via config if schemas enabled) ─
TABLE_PREFIX = config.get("bronze_schema", "")

BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def tbl(name: str) -> str:
    """Qualify table name with schema prefix when running on a schema-enabled lakehouse."""
    return f"{TABLE_PREFIX}.{name}" if TABLE_PREFIX else name

print("Batch      :", BATCH_ID)
print("Repo URL   :", REPO_URL)
print("Clone dir  :", CLONE_DIR)
print("Raw dir    :", RAW_DIR)
print("Table prefix:", TABLE_PREFIX or "(none)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Shallow clone (single packed transfer — far cheaper than 15k HTTPS GETs)

# CELL ********************

# Clean any prior staging clone so the run is idempotent.
if os.path.exists(CLONE_DIR):
    shutil.rmtree(CLONE_DIR)
os.makedirs(os.path.dirname(CLONE_DIR), exist_ok=True)

res = subprocess.run(
    ["git", "clone", "--depth", "1", REPO_URL, CLONE_DIR],
    capture_output=True, text=True
)
print(res.stdout)
print(res.stderr)
res.check_returncode()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 3. Capture provenance (commit SHA) and check size before going further

# CELL ********************

commit_sha = subprocess.run(
    ["git", "-C", CLONE_DIR, "rev-parse", "HEAD"],
    capture_output=True, text=True
).stdout.strip()

size_human = subprocess.run(
    ["du", "-sh", CLONE_DIR], capture_output=True, text=True
).stdout.strip()

print("Cloned commit:", commit_sha)
print("Clone size   :", size_human)
print("\nIf size is multiple GB, confirm notebook disk headroom before the copytree in cell 4.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 4. Persist raw bytes into the Lakehouse Files area
# 
# The clone path is staging; copy `banking_data` into a stable `bronze_raw` location so the PDF/EML bytes survive beyond this session and the manifests point at durable paths.

# CELL ********************

src = os.path.join(CLONE_DIR, REPO_SUBDIR)
assert os.path.isdir(src), f"Expected {src} to exist after clone"

os.makedirs(os.path.dirname(RAW_DIR), exist_ok=True)
shutil.copytree(src, RAW_DIR, dirs_exist_ok=True)

# Free the staging clone (incl. .git history) now that raw bytes are landed.
shutil.rmtree(CLONE_DIR, ignore_errors=True)
print("Raw landing ready at:", RAW_DIR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 5. Helpers — Bronze metadata + path-derived partition columns
# 
# Per the Bronze rules: never modify source columns; add ingestion metadata only. The `year`/`month` come from the **folder path**, not the filename — critical for the sub-folder CSVs which carry no date in their name.

# CELL ********************

# Spark reads the Lakehouse via the relative Files/ path (OneLake-backed).
def with_bronze_meta(df):
    """Add ingestion metadata + path-derived business partition cols. Source columns untouched."""
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
    (df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy(*partition_cols)
        .saveAsTable(name))         
    print(f"  ✅ wrote {name}")


def read_reconciled(spark_paths):
    """Read each parquet file separately, cast type-conflicting columns to STRING, then union.
    Returns (df, conflict_cols)."""
    dfs = [
        spark.read.parquet(p).withColumn("_source_file", F.col("_metadata.file_path"))
        for p in spark_paths
    ]

    types = defaultdict(set)
    for d in dfs:
        for field in d.schema.fields:
            types[field.name].add(field.dataType.simpleString())

    conflict_cols = {c for c, ts in types.items() if len(ts) > 1}

    if conflict_cols:
        dfs = [
            d.select(*[
                F.col(c).cast(StringType()).alias(c) if c in conflict_cols else F.col(c)
                for c in d.columns
            ])
            for d in dfs
        ]

    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs)
    return df, conflict_cols

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Parquet — one Delta table per entity
# 
# Files are named `<entity>_<year>_<month>.parquet`. We group by entity prefix so each becomes its own Bronze table rather than collapsing unrelated schemas into one frame.

# CELL ********************

# SHA + batch ID — reads from saved file if clone is gone
sha_file = os.path.join(RAW_DIR, ".commit_sha")

commit_sha = subprocess.run(
    ["git", "-C", CLONE_DIR, "rev-parse", "HEAD"],
    capture_output=True, text=True
).stdout.strip()

if commit_sha:
    # Clone still exists — save SHA for future runs
    with open(sha_file, "w") as f:
        f.write(commit_sha)
    print(f"commit_sha : {commit_sha} (from git)")
elif os.path.exists(sha_file):
    # Clone is gone but SHA was saved from a previous run
    with open(sha_file) as f:
        commit_sha = f.read().strip()
    print(f"commit_sha : {commit_sha} (from saved file)")
else:
    # Neither — first manual load, no git history
    commit_sha = "pre-landed-manual"
    print(f"commit_sha : {commit_sha} (fallback)")

BATCH_ID = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
print(f"BATCH_ID   : {BATCH_ID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_reconciled(spark_paths):
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

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
    spark_paths = [p.replace("/lakehouse/default/", "") for p in paths]

    if not spark_paths:
        print(f"  WARNING: no files found for entity '{entity}' — skipping")
        continue

    df, conflicts = read_reconciled(spark_paths)

    if conflicts:
        print(f"  {entity}: reconciled columns {sorted(conflicts)}")

    df = with_bronze_meta(df)
    write_bronze(df, f"bronze_{entity}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def with_bronze_meta(df):
    """Add ingestion metadata + path-derived partition cols. Source columns untouched."""
    if "_source_file" not in df.columns:
        df = df.withColumn("_source_file", F.col("_metadata.file_path"))
    return (df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_batch_id",   F.lit(BATCH_ID))
        .withColumn("_commit_sha", F.lit(commit_sha))
        .withColumn("year",  F.regexp_extract(F.col("_source_file"), r"/(\d{4})/", 1))
        .withColumn("month", F.regexp_extract(F.col("_source_file"), r"/\d{4}/(\d{2})/", 1)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 7. JSONL — monthly `transactions.jsonl`
# 
# These live at `<year>/<month>/transactions.jsonl`. Spark reads JSONL natively with `spark.read.json`.

# CELL ********************

def is_lfs_pointer(path, sniff=120):
    try:
        with open(path, "rb") as f:
            return f.read(sniff).startswith(b"version https://git-lfs.github.com/spec/v1")
    except Exception:
        return False

# Derive the raw-download base URL from REPO_URL (already loaded from config).
_repo_base = REPO_URL.replace(".git", "")

pointers = [p for p in glob.glob(f"{RAW_DIR}/*/*/transactions.jsonl") if is_lfs_pointer(p)]
print(f"LFS pointer files to hydrate: {len(pointers)}")

session = requests.Session()
for p in pointers:
    rel = p.split("/bronze_raw/")[1]          # banking_data/2019/01/transactions.jsonl
    url = f"{_repo_base}/raw/main/{rel}"
    with session.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        tmp = p + ".tmp"
        with open(tmp, "wb") as out:
            for chunk in r.iter_content(chunk_size=1 << 20):
                out.write(chunk)
        os.replace(tmp, p)
    print(f"  hydrated {rel} ({os.path.getsize(p):,} bytes)")

# Hard guard: refuse to proceed if anything is still a pointer.
still = [p for p in pointers if is_lfs_pointer(p)]
assert not still, f"Still pointers after download: {still}"

# Ingest all transactions.jsonl into one partitioned Delta table 
jsonl_paths = glob.glob(f"{RAW_DIR}/*/*/transactions.jsonl")
spark_jsonl  = [p.replace("/lakehouse/default/", "") for p in jsonl_paths]

df_txn = (spark.read
          .option("mode", "PERMISSIVE")
          .json(spark_jsonl))
df_txn = df_txn.withColumn("_source_file", F.input_file_name())
df_txn = with_bronze_meta(df_txn)
write_bronze(df_txn, "bronze_transactions")
print(f"Transactions rows: {df_txn.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. CSV — one Delta table per sub-folder type
# 
# CSVs sit in typed sub-folders (`customer_communications/`, `marketing_campaigns/`, `collections_cases/`). **Their filenames carry no date** — the period lives only in the folder path, which `with_bronze_meta` extracts into `year`/`month`. We auto-discover the folder/file combinations rather than hard-coding them, so new folder types in later months are picked up.

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Unstructured — PDF & EML manifests (bytes stay on disk)
# 
# Bronze does **not** parse these. We build a queryable catalog (one row per file) using Spark's `binaryFile` source, which reads path + metadata without interpreting content. The actual bytes remain under `RAW_DIR` for Silver-layer extraction to consume.

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Account counter JSON (single small control file)

# CELL ********************

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
     .saveAsTable("bronze_account_counter"))  

print("✅ wrote bronze_account_counter")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Ingest summary

# CELL ********************

tables = [r.tableName for r in spark.sql("SHOW TABLES").collect() if r.tableName.startswith("bronze_")]
print(f"Commit ingested      : {commit_sha}")
print(f"Batch                : {BATCH_ID}")
print(f"Bronze tables created: {len(tables)}\n")
for t in sorted(tables):
    c = spark.table((t)).count()
    print(f"  {t:55s} {c:>12,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EXPECTED_TABLES = [
    "bronze_accounts",
    "bronze_customers",
    "bronze_atm_logs",
    "bronze_account_limits_history",
    "bronze_debit_orders",
    "bronze_loans",
    "bronze_loan_participations",
    "bronze_rejected_applications",
    "bronze_transactions",
    "bronze_account_counter",
    "bronze_pdf_manifest",
    "bronze_eml_manifest",
]

landed = {r.tableName for r in spark.sql("SHOW TABLES").collect()}
print("=" * 60)
for expected in EXPECTED_TABLES:
    status = "✅" if expected in landed else "❌ MISSING"
    print(f"  {status}  {expected}")
print("=" * 60)

# Flag any expected table that landed empty
for expected in EXPECTED_TABLES:
    if expected in landed:
        cnt = spark.table((expected)).count()
        if cnt == 0:
            print(f"  ⚠️  {expected} landed but has 0 rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1 — Delete the bad landing
import shutil
shutil.rmtree("/lakehouse/default/Files/bronze_raw", ignore_errors=True)
print("✅ Cleared bronze_raw")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

tables = spark.sql("""
SHOW TABLES IN lh_bronze_banking_data_modern_data.dbo
""")

for row in tables.collect():
    table_name = row.tableName
    spark.sql(
        f"DROP TABLE IF EXISTS lh_bronze_banking_data_modern_data.dbo.{table_name}"
    )
    print(f"Dropped: {table_name}")

print("✅ All tables dropped")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
