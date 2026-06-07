# 🏦 Keystone Banking Data Pipeline
### *A Medallion Architecture Implementation on Microsoft Fabric*

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Free%20Trial-blue?style=flat-square&logo=microsoft)](https://fabric.microsoft.com)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Free%20Trial-blue?style=flat-square&logo=microsoft)](https://fabric.microsoft.com)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion-green?style=flat-square)](https://www.databricks.com/glossary/medallion-architecture)
[![Bronze](https://img.shields.io/badge/Bronze%20Layer-Complete-success?style=flat-square)]()
[![Silver](https://img.shields.io/badge/Silver%20Layer-Customers%20%26%20Accounts%20Complete-yellow?style=flat-square)]()
[![Gold](https://img.shields.io/badge/Gold%20Layer-Pending-lightgrey?style=flat-square)]()
[![Dataset](https://img.shields.io/badge/Dataset-GitHub%20Hosted-black?style=flat-square&logo=github)](https://github.com/inhamo/Datasets-Advanced-2026)

---

## 📋 Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Dataset & Source Data](#3-dataset--source-data)
4. [Why Git Clone? — Ingestion Strategy Decision](#4-why-git-clone--ingestion-strategy-decision)
5. [Technology Stack](#5-technology-stack)
6. [Fabric Workspace Structure](#6-fabric-workspace-structure)
7. [Naming Conventions & Standards](#7-naming-conventions--standards)
8. [Data Governance & Quality Framework](#8-data-governance--quality-framework)
9. [Pipeline Execution Guide](#9-pipeline-execution-guide)
10. [Project Timeline & Milestones](#10-project-timeline--milestones)
11. [Risks & Mitigations](#11-risks--mitigations)
12. [Appendices](#12-appendices)

---

## 1. Project Overview

The **Keystone Banking Data Pipeline** is an end-to-end data engineering project that ingests, transforms, and models a multi-year synthetic banking dataset into a structured, analytics-ready lakehouse built on **Microsoft Fabric**.

The platform follows a **Medallion Architecture** (Bronze → Silver → Gold), serving as both a learning implementation and a replicable reference for Fabric-based data engineering. All source data is versioned and hosted on GitHub, and the ingestion strategy is designed to work within the constraints of a **Fabric Free Trial** account.

**Key Goals:**
- Build a durable, partitioned Bronze landing layer from a multi-format dataset (~14,000 files)
- Apply Silver-layer transformations: schema enforcement, deduplication, PII masking, SCD Type 2 tracking
- Expose Gold-layer aggregations for reporting and analytics
- Demonstrate idempotent, reproducible pipeline design on a zero-cost platform

---

## 2. Architecture & Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE LAYER                             │
│   GitHub Repo: inhamo/Datasets-Advanced-2026 (banking_data)     │
│   Parquet · JSONL · CSV · PDF · EML · JSON                      │
└───────────────────────────┬─────────────────────────────────────┘
                            │  git clone --depth 1
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                               │
│   Lakehouse: lh_bronze_banking_data                             │
│   • Raw Delta tables, one per entity                            │
│   • Metadata: _ingest_timestamp, _batch_id, _commit_sha         │
│   • Partitioned by year / month (path-derived)                  │
│   • PDF/EML: raw bytes on Files + manifest Delta tables         │
│   • PII retained in raw form (restricted access)                │
└───────────────────────────┬─────────────────────────────────────┘
                            │  PySpark transformations + PII masking
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                               │
│   • Schema enforcement & explicit typing                        │
│   • Deduplication & null handling                               │
│   • PII masked — first safe layer for downstream consumers      │
│   • SCD Type 2 for slowly changing dimensions                   │
│   • Business rule validation & referential integrity            │
└───────────────────────────┬─────────────────────────────────────┘
                            │  Aggregations & semantic models
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                                │
│   • Dimensional models (fact + dim tables)                      │
│   • Business KPIs and reporting aggregates                      │
│   • Power BI / ML-ready semantic layer                          │
│   • All data already masked from Silver                         │
└─────────────────────────────────────────────────────────────────┘
```

**Processing Windows:**
| Layer | Trigger | Expected Duration |
|---|---|---|
| Bronze | On-demand / SHA-change | ~15–30 min (first run with clone) |
| Silver | After Bronze completes | ~20–40 min |
| Gold | After Silver completes | ~10–20 min |

---

## 3. Dataset & Source Data

**Repository:** [`inhamo/Datasets-Advanced-2026`](https://github.com/inhamo/Datasets-Advanced-2026/tree/main/banking_data)

The dataset is a synthetic multi-year banking dataset spanning **2019–2025**, organized by `year/month` subfolders. It is a **static snapshot** — not a live transactional source — making it ideal for reproducible, commit-pinned ingestion.

| Format | Approx. Count | Bronze Treatment |
|---|---|---|
| Parquet | ~909 files | One Delta table per entity, partitioned by `year`/`month` |
| CSV | ~592 files | One Delta table per subfolder type |
| JSONL | ~87 files | `bronze_transactions` Delta table |
| PDF | ~12,345 files | Raw bytes on Files + `bronze_pdf_manifest` |
| EML | ~915 files | Raw bytes on Files + `bronze_eml_manifest` |

**Key Entities:**
`accounts` · `customers` · `transactions` · `atm_logs` · `loans` · `loan_participations` · `debit_orders` · `account_limits_history` · `rejected_applications` · `account_counter`

> **Note:** `transactions.jsonl` files are stored as Git LFS pointers in the repo. The Bronze notebook automatically detects and hydrates these via direct HTTP download before ingestion.

---

## 4. Why Git Clone? — Ingestion Strategy Decision

This is one of the most deliberate design decisions in the project, and it warrants a clear explanation.

### The Problem: 14,000+ Files, One Free Trial Account

The source data lives on GitHub. On a **Microsoft Fabric Free Trial**, there is no standalone ADLS Gen2 account, no Azure Data Factory, and no Event Hub. The typical enterprise ingestion patterns are unavailable without a paid subscription.

### Options Considered

| Option | Feasibility on Free Trial | Why Rejected / Accepted |
|---|---|---|
| Azure Data Factory HTTP connector | ⚠️ Partial | Serves one file at a time — 14k HTTP calls is impractical |
| Manual upload via Fabric UI | ✅ Works once | Every new file requires manual re-upload — not scalable |
| OneLake direct write (ADLS Gen2 API) | ❌ No standalone account | Requires paid subscription or existing Azure tenant |
| **`git clone --depth 1` in notebook** | ✅ **Chosen** | Single network call, all files land at once, free, reproducible |

### Why Clone Works Well Here

1. **Single packed transfer.** A shallow clone (`--depth 1`) fetches only the latest commit tree — no history, no bloat. This is far cheaper than 14,000 individual HTTPS GETs.

2. **The dataset is a static snapshot.** The repo is not a live transactional system. It's a fixed dataset published for analysis. Cloning the whole thing at once is semantically correct.

3. **Commit SHA = reproducibility.** Every run captures `git rev-parse HEAD` and stores it as `_commit_sha` on every ingested row. This means any result can be traced back to the exact data snapshot it came from — critical for thesis-grade reproducibility.

4. **Change detection prevents redundant clones.** The pipeline checks the remote SHA before cloning:

```python
remote_sha = subprocess.run(
    ["git", "ls-remote", REPO_URL, "HEAD"],
    capture_output=True, text=True
).stdout.split()[0]

# Compare against last ingested SHA in bronze_ingest_control
# If unchanged → exit early, skip clone entirely
```

5. **Works entirely within free tier limits.** No paid services, no API keys, no bandwidth quotas beyond GitHub's standard rate limits.

### The Trade-off Acknowledged

> Git is not a data platform. For a production system with live data, this approach would be replaced by ADLS Gen2 + ADF or an event-driven ingestion pattern. The clone strategy is the right call **for this specific context**: a static, GitHub-hosted research dataset on a free Fabric trial.

---

## 5. Technology Stack

| Category | Technology | Role |
|---|---|---|
| Platform | Microsoft Fabric (Free Trial) | Unified environment for compute, storage, and orchestration |
| Storage | OneLake (via Fabric Lakehouse) | Delta tables + raw Files area — ADLS Gen2 under the hood |
| Processing | PySpark (Fabric Notebooks) | Data ingestion, transformation, Bronze→Silver→Gold |
| Table Format | Delta Lake | ACID transactions, schema evolution, partitioning |
| Orchestration | Fabric Data Pipeline + Task Flow | Scheduling, dependency management, monitoring |
| Version Control | Git / GitHub | Source data hosting + notebook/code versioning |
| Data Ingestion | `git clone --depth 1` via `subprocess` | Single-call full dataset transfer (see Section 4) |
| LFS Hydration | GitHub Raw HTTP + `requests` | Fallback for Git LFS pointer files (JSONL) |
| Reporting | Power BI (via Fabric semantic model) | Gold layer dashboards (Phase 3) |

---

## 6. Fabric Workspace Structure

```
keystone_banking_data  (Workspace)
│
├── 000_config                    Notebook — Config hub, returns JSON to callers
├── 100_001_ingest_banking_data   Notebook — Bronze ingestion (clone → Delta tables)
│
├── lh_bronze_banking_data        Lakehouse
│   ├── Files/
│   │   ├── bronze_raw/
│   │   │   └── banking_data/    Raw bytes (PDF, EML, Parquet, CSV, JSONL)
│   │   └── _staging/            Temporary clone dir (cleaned after each run)
│   └── Tables/
│       ├── bronze_accounts
│       ├── bronze_customers
│       ├── bronze_transactions
│       ├── bronze_atm_logs
│       ├── bronze_loans
│       ├── bronze_loan_participations
│       ├── bronze_debit_orders
│       ├── bronze_account_limits_history
│       ├── bronze_rejected_applications
│       ├── bronze_account_counter
│       ├── bronze_pdf_manifest
│       ├── bronze_eml_manifest
│       └── bronze_ingest_control   ← SHA-based change tracking
│
├── 200_001_transform_customers_silver   Notebook — Silver customers (dim_customers_individual, dim_customers_business)
├── 200_002_transform_accounts_silver    Notebook — Silver accounts (dim_accounts)
│
├── lh_silver_banking_data        Lakehouse
│   └── Tables/
│       ├── dim_customers_individual     ← 80,996 deduplicated individual customers (PII masked)
│       ├── dim_customers_business       ← Business customers (separate schema, PII masked)
│       ├── dim_accounts                 ← 109,841 accounts (CDC-aware merge, PII masked)
│       └── control/
│           ├── batch_watermark          ← High-water mark per pipeline
│           └── silver_audit_log         ← Insert/update counts per batch
│
└── lh_Demo                       Sandbox lakehouse
```

**Task Flow (Fabric UI):**

```
Get Data → Store - Bronze → Prepare to Silver → Store - Silver → Prepare to Gold → Store - Gold
```

---

## 7. Naming Conventions & Standards

Adherence to these conventions is **mandatory** across all layers. Consistency enables automated discovery, lineage tracking, and maintainability.

### General Principles

- Use `snake_case` for all names — tables, columns, files, notebooks
- Avoid SQL reserved keywords (`date` → `transaction_date`, `value` → `account_value`)
- Names must be descriptive without unnecessary abbreviation
- Numbers in notebook names indicate execution order (`100_001_` = Phase 1, Step 1)

---

### Notebook Naming

```
[phase]_[sequence]_[action]_[domain]
```

| Notebook | Purpose |
|---|---|
| `000_config` | Shared configuration — always runs first |
| `100_001_ingest_banking_data` | Bronze ingestion |
| `200_001_transform_[entity]_silver` | Silver transformation per entity |
| `300_001_build_[model]_gold` | Gold layer model build |

---

### Table Naming by Layer

**Bronze (Raw Landing):** `bronze_[entity]`
```
bronze_accounts
bronze_transactions
bronze_pdf_manifest
bronze_ingest_control
```

**Silver (Cleansed & Modelled):** `fact_` / `dim_` prefixes
```
fact_transactions
dim_customers
dim_accounts
fact_loan_performance
```

**Gold (Business Semantics):** `[domain]_[entity]_[purpose]`
```
customer_360_summary
account_risk_profile
transaction_monthly_aggregates
```

---

### Column Naming

| Type | Convention | Example |
|---|---|---|
| General | `snake_case`, singular noun | `account_balance`, `first_name` |
| Surrogate key | `[table]_sk` | `customer_sk`, `account_sk` |
| Natural/business key | `[entity]_id` | `account_id`, `policy_number` |
| Timestamps | `[event]_timestamp` / `[event]_date` | `created_timestamp`, `effective_date` |
| Flags/booleans | `is_[condition]` | `is_active`, `is_current` |
| SCD Type 2 | `valid_from`, `valid_to`, `is_current` | Standard across all SCD2 dims |

**Bronze Technical Columns (added by `with_bronze_meta()`, never modified downstream):**

| Column | Type | Description |
|---|---|---|
| `_source_file` | string | Full path of the originating file |
| `_ingest_timestamp` | timestamp | UTC time the row was ingested |
| `_batch_id` | string | Run identifier (`YYYYMMDDTHHMMSSz`) |
| `_commit_sha` | string | GitHub commit SHA of the cloned snapshot |
| `year` | string | Extracted from file path (`/YYYY/`) |
| `month` | string | Extracted from file path (`/YYYY/MM/`) |

---

## 8. Data Governance & Quality Framework

### Data Classification

| Classification | Examples | Handling |
|---|---|---|
| PII / Confidential | Customer names, account numbers, balances | Masked in Silver; Bronze access-controlled |
| Internal | Operational metrics, batch metadata | Standard access |
| Public | Aggregated reports, anonymized summaries | Unrestricted |

> **Note:** The dataset is synthetic. In a production context, all customer-linked fields would be classified as PII and subject to retention policies and POPIA/GDPR compliance.

---

### PII Masking Strategy

Data masking is applied at the **Silver layer** — the first layer downstream consumers touch. Bronze retains raw values (restricted access). Gold operates entirely on already-masked Silver data.

**Why Silver, not Gold?**
Masking in Silver ensures that *every* Gold table is safe by default, regardless of how many Gold notebooks are written. If masking were deferred to Gold, each new Gold notebook would need to remember to re-apply it — a risk that grows as the project scales.

**Masking is deterministic** — the same input always produces the same masked output. This is achieved using SHA-256 hashing with a project-level salt (`MASK_SALT`, defined in `000_config`). Deterministic masking means:
- Joins across tables still work (e.g. a hashed `id_number` in customers correlates with the same hash in an audit table)
- Duplicate detection still works (two customers with identical emails hash identically — `is_duplicate_email` remains accurate)
- Results are fully reproducible across pipeline runs

**Masked fields by entity:**

`dim_customers_individual` / `dim_customers_business`:

| Column | Technique | Rationale |
|---|---|---|
| `id_number` | Deterministic hash (SHA-256 + salt) | Most sensitive field — SA ID encodes DOB, gender, citizenship. Used as join key in KYC systems so hash must be consistent |
| `tax_id_number` | Deterministic hash (SHA-256 + salt) | Sensitive identifier; not a pipeline join key |
| `email` | Domain-preserving hash | Local part hashed, domain retained — analytically useful (gmail vs corporate) without exposing the individual |
| `phone_number` | Partial mask — keep prefix + last 3 digits | Country code retained for `phone_country_code` derived column; subscriber number removed |
| `residential_address` | Full hash | Free-text field; no structure worth preserving; not a join key |
| `commercial_address` | Full hash | Same as above |
| `next_of_kin` | Full hash | Free-text name field; not used downstream |

`dim_accounts`:

| Column | Technique | Rationale |
|---|---|---|
| `account_number` | Partial mask — last 4 digits visible (`****NNNN`) | Standard banking convention; not a join key |
| `card_number` | Deterministic hash (SHA-256 + salt) | Card network already captured in `card_type` / `card_category` derived columns |
| `iban` | Deterministic hash (SHA-256 + salt) | Not a join key; no structure worth preserving |

**Ordering note for customers notebook:**
`id_dob_match` (SA ID cross-validation) must be computed *before* masking runs, because it reads the first 6 characters of the raw `id_number` to validate the embedded date-of-birth. Once hashed, those characters are gone. The notebook explicitly computes `id_dob_match` on `typed` (pre-mask), then applies masking, then runs all remaining derived columns.

---

### Quality Gates by Layer

**Bronze — Faithful Raw Landing**
- Schema captured as-is (`inferSchema=true` for CSV; explicit for Parquet)
- Type conflicts reconciled by casting conflicting columns to `STRING` (logged)
- LFS pointer guard: hard `assert` blocks pipeline if hydration fails
- Null/completeness checks deferred to Silver
- Expected table checklist validated at end of notebook (Cell 11)
- Empty table detection: warns if any expected table has 0 rows

**Silver — Business Rule Enforcement**
- Explicit schema with enforced types (no `inferSchema`)
- Null classification: required / conditionally null / optional — logged per field
- Deduplication on natural keys (window function ordered by year → month → `_ingest_timestamp`)
- CDC-aware merge for accounts (`cdc_op_hint` I/U resolved via `record_last_updated_at`)
- **PII masking applied before derived columns** (see masking strategy above)
- Delta MERGE (upsert) — no full overwrites; inserts and updates tracked separately
- High-water mark (`control.batch_watermark`) — incremental load on subsequent runs
- Audit log (`control.silver_audit_log`) — rows processed/inserted/updated per batch
- Referential integrity check — `customer_id` in `dim_accounts` validated against `dim_customers`
- Derived segmentation columns: `age_band`, `income_band`, `kyc_risk_tier`, `is_high_risk`, `customer_segment`, `tenure_band`, `account_age_band`, `tier_label`
- Data quality flags: `is_valid_email`, `id_dob_match`, `is_duplicate_email`, `primary_account_violation`, `card_expiring_soon`, `onboarding_doc_score`

**Gold — Consistency & Accuracy**
- Cross-domain join consistency validated
- Aggregation accuracy checks (sum reconciliation)
- Row count variance alerts between Silver and Gold

### DQ Metrics Tracked
`Completeness` · `Accuracy` · `Consistency` · `Timeliness` · `Uniqueness`

---

## 9. Pipeline Execution Guide

### Prerequisites
- Microsoft Fabric workspace with a Lakehouse attached as default
- `000_config` notebook present and returning valid JSON
- Git available in the Fabric notebook runtime (default: yes)
- Internet egress to `github.com` enabled (default: yes)

### First Run

```python
# 000_config returns:
{
  "github_url":    "https://github.com/inhamo/Datasets-Advanced-2026.git",
  "raw_path":      "Files/raw",
  "bronze_schema": "bronze",
  "mask_salt":     "keystone_2026"
}
```

Run notebooks in order:
```
1. 000_config                        (sets config, ~5 sec)
2. 100_001_ingest_banking_data       (clones repo, ingests all formats, ~15–30 min)
```

### Subsequent Runs (Change Detection)

The pipeline checks the remote SHA before cloning. If the repo hasn't changed since the last successful ingest, it exits early:

```
Batch      : 20260602T091500Z
Remote SHA : a3f1bc7...
Last SHA   : a3f1bc7...   ← match
→ No changes detected. Skipping clone.
```

If new files have been pushed to GitHub, the SHA will differ and a full re-clone runs automatically.

### Silver Execution (after Bronze)

Run notebooks in order against `lh_silver_banking_data`:
```
3. 200_001_transform_customers_silver   (~10 min, first run full load)
4. 200_002_transform_accounts_silver    (~8 min, first run full load)
```

Subsequent runs are incremental — only records newer than the last `batch_watermark` are processed. The watermark is updated automatically on each successful run.

### Expected Silver Output

```
✅  dim_customers_individual   ~80,996 rows (deduplicated individuals, PII masked)
✅  dim_customers_business     ~3,426 rows  (deduplicated businesses, PII masked)
✅  dim_accounts               ~109,841 rows (CDC-resolved, PII masked)
✅  control.batch_watermark    audit trail per pipeline
✅  control.silver_audit_log   insert/update counts per batch
```

### Expected Bronze Output

```
✅  bronze_accounts
✅  bronze_customers
✅  bronze_transactions
✅  bronze_atm_logs
✅  bronze_loans
✅  bronze_loan_participations
✅  bronze_debit_orders
✅  bronze_account_limits_history
✅  bronze_rejected_applications
✅  bronze_account_counter
✅  bronze_pdf_manifest
✅  bronze_eml_manifest
```

---

## 10. Project Timeline & Milestones

### Phase 1 — Bronze Foundation ✅ Complete
- Fabric environment and Lakehouse setup
- `000_config` notebook (shared configuration hub)
- Full Bronze ingestion: Parquet, JSONL, CSV, PDF manifest, EML manifest
- SHA-based change detection (skip re-clone if no new commits)
- Idempotent pipeline with expected-table validation

### Phase 2 — Silver Transformation *(In Progress)*
**Complete:**
- `dim_customers_individual` — schema enforcement, dedup, PII masking, derived columns (`age_band`, `income_band`, `kyc_risk_tier`, `customer_segment`, `tenure_band`, surrogate key `customer_sk`)
- `dim_customers_business` — separate schema, business-specific fields only, PII masked
- `dim_accounts` — CDC-aware merge, PII masking, derived columns (`account_age_band`, `tier_label`, `has_overdraft`, `multi_account_flag`, `approval_lag_days`, `card_expiring_soon`)
- `control.batch_watermark` + `control.silver_audit_log` — incremental load + full audit trail
- Email validation, SA ID cross-validation, duplicate email detection, primary account violation flag

**Remaining:**
- `dim_transactions` — fact table for transaction behaviour
- `dim_loans` + `dim_loan_participations`
- `bridge_customer_account` — many-to-many relationship table
- `dim_accounts_status_history` — SCD2 from `status_events_json`

### Phase 3 — Gold & Reporting *(Pending)*
- Dimensional fact/dim models
- KPI aggregations
- Power BI semantic model integration
- ML-ready feature tables

---

## 11. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| GitHub LFS bandwidth exhausted (1GB/month free) | Medium | High | JSONL files hydrated via raw HTTP fallback; LFS usage minimized |
| Fabric Free Trial expiry during development | Medium | High | Export notebooks to GitHub regularly; data re-landable from source |
| Repo structure changes break glob patterns | Low | High | Discover entities dynamically (no hardcoded paths); SHA pinning for thesis runs |
| Schema drift across months (Parquet) | High | Medium | `read_reconciled()` casts conflicting columns to STRING; logged per entity |
| `RAW_DIR` accumulates stale files across runs | Low | Low | Add `shutil.rmtree(RAW_DIR)` before `copytree` to fully reset (recommended) |
| Git not available in Fabric runtime | Very Low | High | Verified available by default; `subprocess.run(["git"...])` confirmed working |
| Masking salt lost or changed between runs | Low | High | Salt defined in `000_config`; changing it invalidates all existing hashes — treat as immutable once Silver is populated |

---

## 12. Appendices

### A. Bronze Metadata Columns Reference

Every Bronze Delta table (except PDF/EML manifests which have their own schema) carries these columns added by `with_bronze_meta()`:

```python
def with_bronze_meta(df):
    return (df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_batch_id",         F.lit(BATCH_ID))         # "20260602T091500Z"
        .withColumn("_commit_sha",       F.lit(commit_sha))       # "a3f1bc7d..."
        .withColumn("year",  F.regexp_extract("_source_file", r"/(\d{4})/", 1))
        .withColumn("month", F.regexp_extract("_source_file", r"/\d{4}/(\d{2})/", 1))
    )
```

### B. Known Data Quirks

- **Schema conflicts in Parquet:** Some entities have type-inconsistent columns across months (e.g., `account_id` as `int` in 2019 and `string` in 2021). These are automatically cast to `STRING` in Bronze and flagged in the run log. Silver enforces the authoritative type.
- **LFS files:** `transactions.jsonl` files are Git LFS pointers. The notebook detects these via a header sniff and hydrates them via direct HTTP before ingestion.
- **CSV filenames carry no date:** The `year`/`month` partition columns are derived entirely from the folder path (`/2019/01/customer_communications/...`), not the filename.

### C. Repo Structure (Source Dataset)

```
Datasets-Advanced-2026/
└── banking_data/
    ├── account_counter.json
    ├── 2019/
    │   ├── 01/
    │   │   ├── accounts_2019_01.parquet
    │   │   ├── customers_2019_01.parquet
    │   │   ├── transactions.jsonl
    │   │   ├── initial_deposits.jsonl
    │   │   ├── customer_communications/
    │   │   │   └── communications.csv
    │   │   └── marketing_campaigns/
    │   │       └── campaigns.csv
    │   └── 02/ ...
    └── 2025/ ...
```

### D. Useful Spark SQL Snippets

```sql
-- Bronze: check all tables and row counts
SHOW TABLES IN bronze;

-- Verify partition coverage for transactions
SELECT year, month, COUNT(*) as rows
FROM bronze.bronze_transactions
GROUP BY year, month
ORDER BY year, month;

-- Confirm commit SHA consistency across a table
SELECT _commit_sha, COUNT(*) as rows
FROM bronze.bronze_accounts
GROUP BY _commit_sha;
```

```sql
-- Silver: check watermark history per pipeline
SELECT pipeline_name, watermark_value, rows_processed, rows_inserted, rows_updated, status
FROM control.batch_watermark
ORDER BY processed_timestamp DESC;

-- Silver: customer segment distribution
SELECT customer_segment, kyc_risk_tier, COUNT(*) as customers
FROM dim_customers_individual
GROUP BY customer_segment, kyc_risk_tier
ORDER BY customers DESC;

-- Silver: accounts per customer
SELECT n_accounts, COUNT(*) as n_customers
FROM (
  SELECT customer_id, COUNT(*) as n_accounts
  FROM dim_accounts
  GROUP BY customer_id
)
GROUP BY n_accounts ORDER BY n_accounts;

-- Silver: primary account violation check
SELECT customer_id, COUNT(*) as primary_count
FROM dim_accounts
WHERE is_primary_account = true
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

### E. Silver Derived Columns Reference

**`dim_customers_individual`**

| Column | Description |
|---|---|
| `customer_sk` | Surrogate key via `xxhash64(customer_id)` |
| `age` | Years from `birth_date` to today |
| `age_band` | 18-24 / 25-34 / 35-44 / 45-54 / 55-64 / 65+ |
| `income_band` | Low / Lower-Middle / Middle / Upper-Middle / High |
| `kyc_risk_tier` | Low / Medium / High / Critical (from `risk_score`) |
| `is_high_risk` | True if PEP, sanctioned country, or Critical risk score |
| `is_foreign_national` | True if `citizenship` ≠ ZA |
| `passport_valid` | True if `expiry_date` > today (Passport holders only) |
| `visa_valid` | True if `visa_expiry_date` > today |
| `customer_segment` | Affluent / Mass Market / Emerging / Business / SME / Corporate |
| `customer_tenure_years` | Years since first appearance in source data |
| `tenure_band` | New / Growing / Established / Loyal |
| `completeness_score` | 0–6 count of non-null contact/identity fields |
| `segmentation_ready` | True if minimum fields for segmentation are present |
| `is_valid_email` | Regex validation of masked email format |
| `phone_country_code` | Extracted from masked `phone_number` prefix (ZA/KE/LS/ZW/OTHER) — partial mask preserves prefix |
| `id_dob_match` | SA ID cross-validation: computed on raw `id_number` **before** masking |
| `is_duplicate_email` | True if same email appears on more than one customer — works on masked value (deterministic hash) |

**`dim_accounts`**

| Column | Description |
|---|---|
| `account_sk` | Surrogate key via `xxhash64(account_id)` |
| `account_age_days` | Days from `opening_date` to today |
| `account_age_band` | New / Recent / Established / Mature |
| `is_active` / `is_inactive` / `is_at_risk` | Derived from `account_status` |
| `tier_label` | Human-readable tier description |
| `has_overdraft` / `has_credit_card` | Feature flags from limit columns |
| `is_foreign_currency` | True if `currency` ∈ {USD, EUR} |
| `is_joint_account` / `is_business_account` | Type flags |
| `card_valid` / `card_category` | Card expiry status + credit/debit classification |
| `card_expiring_soon` | True if card expires within 90 days |
| `onboarding_doc_score` | 0–7 count of onboarding documents provided |
| `days_since_status_change` | Days since account status last changed |
| `approval_lag_days` | `approval_date` − `opening_date` |
| `multi_account_flag` | True if customer holds more than one account |
| `primary_account_violation` | True if customer has more than one primary account |

---

*Built on Microsoft Fabric · Delta Lake · PySpark · GitHub*
*Dataset: [`inhamo/Datasets-Advanced-2026`](https://github.com/inhamo/Datasets-Advanced-2026)*
