# KEYSTONE BANK DATA END-TO-END PIPELINE

## Table of Contents

1. [Project Context & Vision](#1-project-context--vision)  
2. [Business Success Criteria](#2-business-success-criteria)  
3. [Inventory Resources](#3-inventory-resources)  
4. [Tech Stack](#4-tech-stack)  
5. [Business Assumptions](#5-business-assumptions)  
6. [Data Assumptions](#6-data-assumptions)  
7. [Constraints](#7-constraints)  
8. [Appendices](#8-appendices)

---

## 1. Project Context & Vision

- **Project Name:** KeyStone Bank Analytics Platform  
- **Vision:** Build a modern banking data platform that turns raw banking data into trusted insights, reports, dashboards, and future data science use cases.  
- **Core Problem Statement:** Banking data is often spread across different systems such as customers, accounts, transactions, loans, cards, branches, and digital channels. Because the data is separated, it becomes difficult to get one trusted view of the business, build reliable reports, detect risks early, and support better decision-making.

### 1.1 Background

The KeyStone Bank data engineering and analytics project is a learning-focused but realistic initiative to design and build an end-to-end banking data platform using Microsoft Fabric concepts.

The aim is to create a central place where banking data can be collected, cleaned, organized, modeled, and made ready for reporting, analysis, and future machine learning work. Instead of having separate files, disconnected tables, and manual reports, this project will show how a real data team could move from raw banking data into trusted business-ready outputs.

The platform will support core banking areas such as:

- Customer analytics
- Account and transaction analysis
- Loan and repayment monitoring
- Branch or channel performance
- Risk and compliance reporting

In simple terms, this project is about learning how to take messy banking data and turn it into a structured, trusted, and useful data platform.

### 1.2 Business Objective

**Vision:**  
To help KeyStone Bank become more data-driven by creating a secure, scalable, and trusted analytics platform that transforms raw banking data into clear business insights.

**Main Objective:**  
To design and build a Microsoft Fabric-based data pipeline that moves data through clear stages:

```text
Raw data
→ Cleaned data
→ Business-ready data
→ Data models
→ Dashboards and analysis
```

The project will focus on:

- Creating a single trusted view of important banking data
- Organizing data into Bronze, Silver, and Gold layers
- Designing useful data models for reporting and analysis
- Building outputs that analysts, managers, and future data science teams can use
- Practicing real-world data engineering, business analysis, data architecture, and analytics documentation

### 1.3 Key Business Objectives

The initial banking problems we may focus on include:

1. Credit risk modelling
2. Revenue reporting
3. Churn prediction and descriptive analysis
4. Customer segmentation

This is a rough starting list of common banking use cases. The list will be updated as the project continues, especially as new business problems are discovered from email folders and available datasets.

---

## 2. Business Success Criteria

The KeyStone Bank Analytics Platform will be considered successful if it delivers measurable business value, trusted data, and useful reporting outputs.

---

## 3. Inventory Resources

This section lists the resources needed to plan, build, manage, and deliver the KeyStone Bank Analytics Platform.

### 3.1 People Resources

| Name | Email | Role | Team / Department | Area of Inquiry |
|---|---|---|---|---|
| Vincent Chitsike | vincent@takueinno.onmicrosoft.com | Data Engineer (Overall Team Lead) | Data Engineering | Project direction, priorities, approvals |
| Nomusa Lembede | nomusa@takueinno.onmicrosoft.com | Analytics Engineer (Team Lead) | Analytics | Pipelines, ingestion, storage, transformations |
| Innocent Nhamo | takueinno@takueinno.onmicrosoft.com | Data Analyst | Analytics | Pipelines, ingestion, storage, transformations |
| Fulufhelo Shavhani | fulufhelo@takueinno.onmicrosoft.com | ML Ops (Team Lead) | Data | Reports, dashboards, KPIs, analysis |
| Tebatso Mamabolo (/θvatso) | tebatso@takueinno.onmicrosoft.com | Business Intelligence Analyst | Data | Reports, dashboards, KPIs, analysis |
| Tebogo Lesedi | tebogo@takueinno.onmicrosoft.com | ML Ops | Data | Reports, dashboards, KPIs, analysis |
| Uhone Rasifudi | uhone@takueinno.onmicrosoft.com | Data Analyst | Data | Reports, dashboards, KPIs, analysis |

### 3.2 Technology Resources

| Resource | Purpose |
|---|---|
| Microsoft Fabric | Main platform for the end-to-end data solution |
| OneLake | Central storage layer for project data |
| Fabric Lakehouse | Stores raw, cleaned, and business-ready data |
| Fabric Warehouse | Optional SQL-friendly serving layer |
| Data Factory Pipelines | Moves and orchestrates data |
| Fabric Notebooks | Used for ingestion, cleaning, transformation, and exploration |
| SQL | Used for querying, modeling, and validation |
| Power BI | Used for dashboards, reports, and semantic models |
| GitHub | Stores project code, documentation, and version history |
| Excel / CSV / Parquet / JSON / PDF / TXT files | Possible starting data sources |
| Draw.io / Miro / Visio / PowerPoint / Excalidraw | Used for architecture and data model diagrams |

### 3.3 Data Resources

| Data resource | Possible examples |
|---|---|
| Customer data | Customer ID, name, age band, segment, KYC status |
| Account data | Account ID, account type, balance, open date, status |
| Transaction data | Transaction ID, date, amount, type, merchant, channel |
| Loan data | Loan ID, customer ID, loan amount, repayment status, arrears |

### 3.4 Documentation Resources

| Document | Purpose |
|---|---|
| Project README | Explains the project purpose, scope, and structure |

### 3.5 Environment Resources

| Environment | Purpose |
|---|---|
| Git repository | Used for code and documentation control |
| Power BI workspace | Used to publish semantic models and dashboards |

### 3.6 Learning Resources

| Resource | Purpose |
|---|---|
| Microsoft Learn | Learn Microsoft Fabric, Power BI, SQL, and data engineering concepts |
| Fabric documentation | Understand Fabric features and platform setup |
| Power BI documentation | Learn dashboards, semantic models, DAX, and reports |
| SQL practice resources | Improve querying and validation skills |
| Data modeling books | Understand star schema, ER modeling, Data Vault, and dimensional modeling |
| Banking analytics examples | Understand common banking KPIs and use cases |
| GitHub portfolio examples | Learn how to package the project professionally |

### 3.7 Security And Compliance Resources

| Resource | Purpose |
|---|---|
| Access control rules | Defines who can view or edit data |
| Data classification | Labels data as public, internal, confidential, or restricted |
| PII handling rules | Controls sensitive customer information |
| Masking / anonymization approach | Protects personal banking data |
| Audit log | Tracks changes and access |
| Compliance checklist | Ensures the project follows basic banking data controls |
| Retention rules | Defines how long data should be kept |

### 3.8 Output Resources

| Output | Purpose |
|---|---|
| Bronze tables | Raw ingested data |
| Silver tables | Cleaned and standardized data |
| Gold tables | Business-ready data |
| Star schema | Analytics-ready fact and dimension model |
| Semantic model | Shared Power BI business layer |
| Dashboard | Final business-facing report |

---

## 4. Tech Stack

### 4.1 Data Engineering Stack

| Area | Tools / Approach | Purpose |
|---|---|---|
| Ingestion | Microsoft Fabric Data Factory Pipelines, Notebooks | Bring data from files, folders, APIs, or databases into the platform |
| Orchestration | Data Factory Pipelines | Schedule and control how data moves through the pipeline |
| Storage | OneLake, Fabric Lakehouse, Delta / Parquet tables | Store raw, cleaned, and business-ready data |
| Processing | Fabric Notebooks, Spark, SQL | Clean, transform, join, and prepare datasets |
| Data Quality | SQL checks, notebook checks, validation tables | Detect missing values, duplicates, invalid records, and broken relationships |

### 4.2 Data Analysis Stack

| Area | Tools / Approach | Purpose |
|---|---|---|
| Semantic Model | Power BI Semantic Model | Create shared business measures and relationships |
| Reporting | Power BI | Build dashboards and reports for business users |
| Analysis | SQL, Power BI, Excel | Explore trends, compare performance, and answer business questions |
| KPI Definition | DAX measures, metric dictionary | Keep calculations consistent across reports |
| Visualization | Power BI charts, tables, slicers, drill-through pages | Help users understand the data clearly |

### 4.3 Documentation And Collaboration Stack

| Area | Tools / Approach | Purpose |
|---|---|---|
| Version Control | GitHub | Store project code, documentation, and progress history |
| Diagrams | Draw.io, Visio, PowerPoint, Excalidraw | Create architecture, pipeline, and data model diagrams |
| Documentation | Markdown, PDF, Excel | Document requirements, decisions, definitions, and deliverables |
| Project Tracking | Excel, GitHub Projects, Planner, or simple task board | Track work, owners, risks, and progress |

---

## 5. Business Assumptions

1. The bank needs a trusted data platform to improve reporting, analysis, and future data science work.
2. Business users need clearer visibility into customers, accounts, transactions, loans, and performance.
3. Some project requirements will be discovered later from email folders, stakeholder requests, and available datasets.
4. Power BI dashboards will be one of the main outputs for business users.
5. The project is both a learning exercise and a realistic portfolio-style banking data solution.

---

## 6. Data Assumptions

1. The project will include banking-related datasets such as customers, accounts, transactions, loans, and possibly branches or products.
2. Some source data may arrive as files such as CSV, Excel, JSON, TXT, PDF, or Parquet.
3. The raw data may contain missing values, duplicates, inconsistent formats, and unclear business definitions.
4. Sensitive customer or financial information may need to be masked, removed, anonymized, or handled carefully.
5. The data will be organized into Bronze, Silver, and Gold layers before being used for reporting or analysis.

---

## 7. Constraints

1. The project team is still learning Microsoft Fabric, data engineering, data modeling, and Power BI.
2. Not all requirements are known at the beginning because some business problems may come from emails or later analysis.
3. The project may use sample, synthetic, or anonymized data instead of real banking production data.
4. Time, access, and technical skill level may limit how advanced the first version can be.
5. The first version should focus on a clear, working end-to-end solution rather than trying to solve every possible banking use case.

---

## 8. Appendices

### Appendix A: Key Terms

| Term | Simple Meaning |
|---|---|
| Bronze Layer | Raw data stored as received from the source |
| Silver Layer | Cleaned and standardized data |
| Gold Layer | Business-ready data used for reporting and analysis |
| Lakehouse | A storage and analytics environment that supports files, tables, and large-scale data processing |
| Semantic Model | A business-friendly layer that defines relationships, measures, and calculations for Power BI |
| Star Schema | A data model with fact tables and dimension tables, commonly used for analytics |
| Fact Table | A table that stores measurable business events, such as transactions or loan repayments |
| Dimension Table | A table that describes business context, such as customer, product, branch, or date |
| KPI | A key performance indicator used to measure business performance |
| Data Product | A trusted, reusable data output with a clear purpose and owner |

### Appendix B: Possible Banking Use Cases

1. Customer segmentation
2. Credit risk modelling
3. Churn prediction
4. Revenue reporting
5. Transaction monitoring
6. Loan repayment analysis
7. Branch performance reporting
8. Product performance analysis
9. Fraud pattern detection
10. Customer 360 reporting

### Appendix C: Possible Project Deliverables

1. Project README
2. Requirements document
3. Data source inventory
4. Data architecture diagram
5. Bronze, Silver, and Gold tables
6. Data quality checks
7. Data model diagram
8. Power BI semantic model
9. Power BI dashboard
10. Portfolio case study

### Appendix D: Open Questions

| Question | Owner | Status |
|---|---|---|
| What datasets are available for the first version? | Data Team | Open |
| Which banking use case should be built first? | Team Lead | Open |
| Which KPIs must be defined first? | Data Analyst | Open |
| Will the project use real, synthetic, or anonymized data? | Team Lead | Open |
| Which dashboard should be delivered first? | Data Analyst | Open |
