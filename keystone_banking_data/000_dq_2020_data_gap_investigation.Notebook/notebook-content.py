# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3eb590da-d3b9-45f8-8f0e-1171e9ac479e",
# META       "default_lakehouse_name": "lh_bronze_banking_data",
# META       "default_lakehouse_workspace_id": "ac490e92-90f3-41a9-82ae-825ecaa77238",
# META       "known_lakehouses": [
# META         {
# META           "id": "3eb590da-d3b9-45f8-8f0e-1171e9ac479e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import os, glob, requests
from pyspark.sql import functions as F

RAW_DIR   = "/lakehouse/default/Files/bronze_raw/banking_data"
REPO_BASE = "https://raw.githubusercontent.com/inhamo/Datasets-Advanced-2026/main/banking_data"

# The 5 entities known to have empty files in 2020
GAP_ENTITIES = [
    "account_limits_history",
    "account_product_enrollments",
    "account_signatories",
    "account_status_events",
    "accounts",
]

# Months where the gap was detected
GAP_MONTHS = ["01", "02", "03", "04", "05"]
GAP_YEAR   = "2020"

print("Setup complete.")
print(f"Investigating: {GAP_YEAR} months {GAP_MONTHS[0]}–{GAP_MONTHS[-1]}")
print(f"Entities     : {GAP_ENTITIES}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{'File':<65} {'Size (bytes)':>12} {'On Disk?':>10}")
print("-" * 90)

file_report = []

for month in GAP_MONTHS:
    for entity in GAP_ENTITIES:
        filename  = f"{entity}_{GAP_YEAR}_{month}.parquet"
        full_path = f"{RAW_DIR}/{GAP_YEAR}/{month}/{filename}"
        exists    = os.path.exists(full_path)
        size      = os.path.getsize(full_path) if exists else 0
        file_report.append({
            "year": GAP_YEAR, "month": month, "entity": entity,
            "filename": filename, "exists": exists, "size_bytes": size
        })
        status = "✅" if exists else "❌ MISSING"
        print(f"{filename:<65} {size:>12,} {status:>10}")

print(f"\nTotal files checked: {len(file_report)}")
print(f"Present on disk    : {sum(1 for r in file_report if r['exists'])}")
print(f"Missing from disk  : {sum(1 for r in file_report if not r['exists'])}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{'File':<65} {'Header':>8} {'Size':>8} {'Verdict'}")
print("-" * 100)

for r in file_report:
    if not r["exists"]:
        print(f"{r['filename']:<65} {'N/A':>8} {'0':>8} ❌ NOT ON DISK")
        continue

    full_path = f"{RAW_DIR}/{r['year']}/{r['month']}/{r['filename']}"
    with open(full_path, "rb") as f:
        header = f.read(4)

    if header == b"PAR1":
        verdict = "✅ Valid parquet (but may be empty)"
    elif header[:4] == b"vers":
        verdict = "⚠️  LFS pointer — not hydrated"
    else:
        verdict = f"❓ Unknown header: {header}"

    print(f"{r['filename']:<65} {str(header):>8} {r['size_bytes']:>8,} {verdict}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{'File':<65} {'Columns':>8} {'Verdict'}")
print("-" * 95)

schema_report = []

for r in file_report:
    if not r["exists"]:
        continue

    spark_path = f"Files/bronze_raw/banking_data/{r['year']}/{r['month']}/{r['filename']}"
    try:
        df       = spark.read.parquet(spark_path)
        n_cols   = len(df.columns)
        verdict  = "✅ Has schema" if n_cols > 0 else "❌ EMPTY SCHEMA (0 columns)"
        schema_report.append({**r, "n_cols": n_cols, "schema_ok": n_cols > 0})
    except Exception as e:
        n_cols  = -1
        verdict = f"❌ UNREADABLE: {str(e)[:60]}"
        schema_report.append({**r, "n_cols": -1, "schema_ok": False})

    print(f"{r['filename']:<65} {n_cols:>8} {verdict}")

empty  = sum(1 for s in schema_report if s["n_cols"] == 0)
broken = sum(1 for s in schema_report if s["n_cols"] == -1)
good   = sum(1 for s in schema_report if s["n_cols"] > 0)
print(f"\nEmpty schema (0 cols) : {empty}")
print(f"Unreadable            : {broken}")
print(f"Valid with schema     : {good}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"{'File':<65} {'HTTP':>6} {'Size':>8} {'Header':>8} {'Source verdict'}")
print("-" * 110)

for r in file_report:
    url = f"{REPO_BASE}/{r['year']}/{r['month']}/{r['filename']}"
    try:
        resp   = requests.get(url, timeout=30)
        size   = len(resp.content)
        header = resp.content[:4]

        if resp.status_code == 404:
            verdict = "❌ File does not exist in repo"
        elif header == b"PAR1" and size < 1000:
            verdict = "❌ SOURCE IS EMPTY — confirmed repo-side gap"
        elif header == b"PAR1":
            verdict = "✅ Source has real data"
        elif header[:4] == b"vers":
            verdict = "⚠️  Source serves LFS pointer"
        else:
            verdict = f"❓ Unknown: {header}"

        print(f"{r['filename']:<65} {resp.status_code:>6} {size:>8,} {str(header):>8} {verdict}")

    except Exception as e:
        print(f"{r['filename']:<65} {'ERR':>6} {'N/A':>8} {'N/A':>8} ❌ Request failed: {str(e)[:40]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build coverage matrix
all_months = [f"{m:02d}" for m in range(1, 13)]

print("2020 Coverage Matrix — ✅ Data present | ❌ Empty/missing")
print()
print(f"{'Entity':<45}" + "".join(f" {m:>4}" for m in all_months))
print("-" * 110)

for entity in GAP_ENTITIES:
    row = f"{entity:<45}"
    for month in all_months:
        filename  = f"{entity}_{GAP_YEAR}_{month}.parquet"
        full_path = f"{RAW_DIR}/{GAP_YEAR}/{month}/{filename}"
        if not os.path.exists(full_path):
            row += "  ❓ "   # file not on disk at all
        else:
            size = os.path.getsize(full_path)
            row += "  ✅ " if size > 1000 else "  ❌ "
    print(row)

print()
print("Legend: ✅ = real data  |  ❌ = empty parquet shell  |  ❓ = file not present")
print("Note  : Only months 01–05 are affected. Months 06–12 were not checked (outside gap window).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for entity in GAP_ENTITIES:
    table = f"bronze_{entity}"
    try:
        df       = spark.table(table)
        total    = df.count()
        by_year  = (df.groupBy("year", "month")
                      .count()
                      .orderBy("year", "month"))

        in_2020  = df.filter(F.col("year") == "2020").count()

        print(f"\n{'='*60}")
        print(f"  {table}")
        print(f"  Total rows : {total:,}")
        print(f"  2020 rows  : {in_2020:,}")
        if in_2020 == 0:
            print(f"  ❌ NO 2020 DATA in this table")
        else:
            print(f"  ✅ 2020 data present ({in_2020:,} rows)")
        print(f"  Year/Month breakdown:")
        by_year.filter(F.col("year") == "2020").show(15, truncate=False)

    except Exception as e:
        print(f"\n❌ Could not query {table}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime

print("=" * 70)
print("  DATA QUALITY FINDING — BRONZE LAYER")
print("=" * 70)
print(f"""
  Finding ID   : DQ-BRONZE-001
  Severity     : Medium (source gap — pipeline unaffected)
  Detected     : {datetime.datetime.utcnow().strftime('%Y-%m-%d')}
  Detected by  : 000_DQ_2020_data_gap_investigation notebook
  Dataset      : inhamo/Datasets-Advanced-2026 (banking_data)

  DESCRIPTION
  -----------
  24 parquet files in the source GitHub repository contain valid
  parquet headers (PAR1 magic bytes) but carry zero columns and
  zero rows. These are empty parquet shells, not LFS pointer files.
  The GitHub API serves these files with HTTP 200 and correct
  Content-Type, making them indistinguishable from real files
  until schema inspection.

  AFFECTED FILES
  --------------
  Year  : 2020
  Months: 01, 02, 03, 04, 05
  Entities:
    - account_limits_history
    - account_product_enrollments
    - account_signatories
    - account_status_events
    - accounts (months 02–05 only)

  ROOT CAUSE
  ----------
  Empty parquet shells were committed to the source repository.
  This is a source data quality issue — not a pipeline or
  ingestion error. Re-cloning or re-downloading produces the
  same result.

  PIPELINE HANDLING
  -----------------
  The Bronze ingestion notebook (100_001_ingest_banking_data)
  detects empty schemas via read_reconciled() and skips affected
  files with a ⚠️ warning. All 11 Bronze tables were created
  successfully using the remaining 885 valid files.

  DOWNSTREAM IMPACT
  -----------------
  Silver and Gold layers will have no data for the affected
  entity/month combinations in 2020 months 01–05. Any analytics
  or reporting that compares 2019 vs 2020 year-over-year trends
  for these entities should note this gap explicitly.

  STATUS
  ------
  Accepted — source cannot be modified (external GitHub repo).
  Documented in README (Section 11) and this notebook.
  Silver layer to handle missing partitions gracefully.
""")
print("=" * 70)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

gap_records = []
for r in file_report:
    gap_records.append((
        r["year"],
        r["month"],
        r["entity"],
        r["filename"],
        r["exists"],
        r["size_bytes"],
        "DQ-BRONZE-001",
        "empty_parquet_shell",
        "source_gap"
    ))

schema = StructType([
    StructField("year",           StringType(),  False),
    StructField("month",          StringType(),  False),
    StructField("entity",         StringType(),  False),
    StructField("filename",       StringType(),  False),
    StructField("on_disk",        BooleanType(), False),
    StructField("size_bytes",     IntegerType(), False),
    StructField("finding_id",     StringType(),  False),
    StructField("gap_type",       StringType(),  False),
    StructField("resolution",     StringType(),  False),
])

gap_df = spark.createDataFrame(gap_records, schema)

(gap_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_dq_gap_manifest"))

print(f"✅ Saved bronze_dq_gap_manifest ({gap_df.count()} records)")
display(gap_df.orderBy("month", "entity"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
