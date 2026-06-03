# 🏦 Keystone Banking Data Pipeline
### *A Medallion Architecture Implementation on Microsoft Fabric*

[![Architecture](https://img.shields.io/badge/Architecture-Medallion-green?style=flat-square)](https://www.databricks.com/glossary/medallion-architecture)
[![Bronze](https://img.shields.io/badge/Bronze%20Layer-Complete-success?style=flat-square)]()
[![Silver](https://img.shields.io/badge/Silver%20Layer-In%20Progress-yellow?style=flat-square)]()
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
- Apply Silver-layer transformations: schema enforcement, deduplication, SCD Type 2 tracking
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
└───────────────────────────┬─────────────────────────────────────┘
                            │  PySpark transformations
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                               │
│   • Schema enforcement & explicit typing                        │
│   • Deduplication & null handling                               │
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
├── lh_silver_banking_data        Lakehouse (Silver — In Progress)
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

### Fabric Item Naming

| Item Type | Convention | Example |
|---|---|---|
| Workspace | `wk_[project]_[domain]` | `wk_keystone_banking` |
| Lakehouse | `lh_[layer]_[domain]` | `lh_bronze_banking_data` |
| Notebook | `[phase]_[seq]_[action]_[domain]` | `100_001_ingest_banking_data` |
| Pipeline | `pl_[action]_[target]_[layer]` | `pl_ingest_banking_bronze` |
| Dataflow Gen2 | `df_[action]_[layer]_[data]` | `df_transform_silver_accounts` |
| Stored Procedure | `pcd_[action]_[target]_[layer]` | `pcd_merge_dim_customer_silver` |

---

## 8. Data Governance & Quality Framework

### Data Classification

| Classification | Examples | Handling |
|---|---|---|
| PII / Confidential | Customer names, account numbers, balances | Masked in Gold; access-controlled |
| Internal | Operational metrics, batch metadata | Standard access |
| Public | Aggregated reports, anonymized summaries | Unrestricted |

> **Note:** The dataset is synthetic. In a production context, all customer-linked fields would be classified as PII and subject to retention policies and POPIA/GDPR compliance.

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
- Null handling: required fields must be non-null
- Deduplication on natural keys
- Referential integrity between entities
- SCD Type 2 tracking (`valid_from`, `valid_to`, `is_current`) on slowly changing dimensions

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
  "bronze_schema": "bronze"
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
- Explicit schema enforcement per entity
- SCD Type 2 implementation for customer and account dimensions
- Deduplication and null handling
- Referential integrity validation
- Silver lakehouses for each domain

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
| **2020 data gap (months 01–05)** | Confirmed | Medium | DQ-BRONZE-001: `account_limits_history`, `account_product_enrollments`, `account_signatories`, `account_status_events`, and `accounts` have no data for 2020 months 01–05. Source repo serves empty parquet shells. Months 06–12 present. Logged in `bronze_dq_gap_manifest`. Silver handles missing partitions gracefully. |

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
- **2020 data gap (DQ-BRONZE-001):** `account_limits_history`, `account_product_enrollments`, 
  `account_signatories`, `account_status_events`, and `accounts` contain no data for 2020 months 
  01–05. The source GitHub repo serves valid PAR1 parquet headers with `"columns": []` — empty 
  shells, not LFS pointers. Confirmed by `000_DQ_2020_data_gap_investigation` notebook. 
  Months 06–12 of 2020 are present and ingested normally. A `bronze_dq_gap_manifest` Delta 
  table records all 24 affected files for Silver/Gold reference. Any year-over-year analytics 
  comparing 2019 vs 2020 for these entities should exclude months 01–05 from 2020.

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
-- Check all Bronze tables and row counts
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

---

*Built on Microsoft Fabric · Delta Lake · PySpark · GitHub*
*Dataset: [`inhamo/Datasets-Advanced-2026`](https://github.com/inhamo/Datasets-Advanced-2026)*
