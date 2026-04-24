
# 🏦 Credit Risk & Loan Default Analytics Platform

<div align="center">

![SQL Server](https://img.shields.io/badge/SQL%20Server-2019+-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![SSIS](https://img.shields.io/badge/SSIS-ETL%20Orchestration-0078D4?style=for-the-badge&logo=microsoft&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Reporting-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Git](https://img.shields.io/badge/Git-GitFlow-F05032?style=for-the-badge&logo=git&logoColor=white)

![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen?style=for-the-badge)
![Tests](https://img.shields.io/badge/Tests-41%2F41%20Passing-success?style=for-the-badge)
![Rows](https://img.shields.io/badge/Dataset-158%2C149%20Rows-blueviolet?style=for-the-badge)
![License](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)

<br/>

> **A production-grade Banking Credit Risk Data Platform** simulating how banks manage loan exposure, default risk, delinquency trends, and portfolio monitoring — built with SQL, SSIS, Python, and SSMS.
</div>

---

## 📋 Table of Contents
- [Business Problem](#-business-problem)
- [Solution Architecture](#-solution-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Star Schema](#-star-schema)
- [Dataset](#-dataset)
- [Key Features](#-key-features)
- [Analytics Layer](#-analytics-layer)
- [Regulatory Alignment](#-regulatory-alignment)
- [Test Results](#-test-results)

---

## 🏦 Business Problem

Banks must continuously answer critical questions about their loan portfolios:

| Question | Without This Platform |
|---|---|
| Which customers are likely to default? | Discovered reactively — after the loss |
| What is total Exposure at Default (EAD)? | Manual spreadsheets, days delayed |
| Where is portfolio concentration growing? | No early warning capability |
| Which accounts need early intervention? | Collections teams work blind |
| What are IFRS 9 ECL provisions? | Estimated, never data-driven |

---

## 🏗️ Solution Architecture

```
╔══════════════════════════════════════════════════════════════════╗
║              MEDALLION ARCHITECTURE — CREDIT RISK                ║
╠══════════════════════════════════════════════════════════════════╣
║  SOURCE SYSTEMS                                                  ║
║  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          ║
║  │  Core    │  │   Loan   │  │Collections│  │   CRM    │         ║
║  │ Banking  │  │   Orig   │  │  System  │  │          │          ║
║  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘          ║
║       └─────────────┴─────────────┴──────────────┘               ║
║                              │ CSV Extract                       ║
║                              ▼                                   ║
║  🥉 BRONZE ── Raw ingestion, audit trail, duplicate flagging     ║
║                              │                                   ║
║                              ▼                                   ║
║  🥈 SILVER ── Type cast, validated, DQ-flagged, MERGE upserts    ║
║                              │                                   ║
║                              ▼                                   ║
║  🥇 GOLD ─── Star schema, SCD2, PD/LGD/EAD/ECL/IFRS9 Stage       ║
║                              │                                   ║
║                              ▼                                   ║
║  📊 ANALYTICS ── 6 reporting views → Power BI / SSRS             ║
╚══════════════════════════════════════════════════════════════════╝
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Database** | SQL Server 2019+ | Core data warehouse engine |
| **ETL** | SSIS | Pipeline orchestration and scheduling |
| **Data Generation** | Python 3.11 | Realistic 158K+ row dataset |
| **Source Control** | Git / GitFlow | Branch-per-feature, approval gates |
| **Reporting** | Power BI + SSRS | Executive dashboards + regulatory reports |
| **Deployment** | PowerShell 7.x | Automated ordered schema deployment |
| **Testing** | pytest | 41 unit tests, all green |

---

## 📁 Project Structure

```
Credit-Risk-Platform/
├── 📂 datasets/
│   └── source_files/               ← 158,149 rows across 5 CSV files
├── 📂 scripts/
│   └── generate_datasets.py        ← Realistic SA banking dataset generator
├── 📂 sql/
│   ├── bronze/
│   │   ├── 01_bronze_schema_and_tables.sql
│   │   └── 02_bronze_bulk_insert_fixed.sql   ← Staging-table pattern (Msg 4864 fix)
│   ├── silver/
│   │   ├── 01_silver_schema_and_tables.sql
│   │   └── 02_silver_transformations.sql
│   ├── gold/
│   │   ├── 01_gold_star_schema.sql
│   │   └── 02_gold_scd2_and_loads.sql
│   ├── analytics/
│   │   └── 01_analytics_views.sql            ← 6 executive reporting views
│   └── audit_logging/
│       └── 01_audit_control_tables.sql
├── 📂 docs/
│   ├── architecture/ARCHITECTURE.md
│   ├── business_rules/BUSINESS_RULES.md
│   └── data_dictionary/DATA_DICTIONARY.md
├── 📂 tests/
│   └── test_dataset_generation.py            ← 41 unit tests
├── requirements.txt
└── README.md
```

---

## ⭐ Star Schema

```
                    ┌─────────────────────┐
                    │      dim_date        │
                    │  date_key (PK)       │
                    │  full_date           │
                    │  month_name          │
                    │  fiscal_year (SA)    │
                    └──────────┬──────────┘
                               │
┌──────────────────┐  ┌────────┴───────────────────┐  ┌─────────────────────┐
│  dim_customer    │  │    fact_credit_risk  🌟     │  │     dim_loan        │
│  ─────────────   │  │  ─────────────────────────  │  │  ────────────────   │
│  customer_key(PK)├──│  risk_fact_id (PK)           ├──│  loan_key (PK)      │
│  customer_id(BK) │  │  customer_key (FK)           │  │  loan_id (BK)       │
│  income_band     │  │  loan_key (FK)               │  │  loan_type          │
│  credit_risk_band│  │  repayment_key (FK)          │  │  loan_amount        │
│  ── SCD Type 2 ──│  │  date_key (FK)               │  │  collateral_type    │
│  effective_date  │  │  exposure_amount             │  │  loan_size_band     │
│  expiry_date     │  │  probability_of_default      │  │  is_secured         │
│  is_current      │  │  loss_given_default          │  └─────────────────────┘
│  version_number  │  │  exposure_at_default         │
└──────────────────┘  │  expected_credit_loss        │  ┌─────────────────────┐
                      │  ecl_stage (IFRS9)            ├──│ dim_repayment_status│
                      │  dpd_bucket                   │  │  repayment_key (PK) │
                      │  default_flag                 │  │  repayment_status   │
                      │  npl_flag                     │  │  status_category    │
                      └───────────────────────────────┘  │  risk_weight        │
                                                          └─────────────────────┘
```

---

## 📊 Dataset

| File | Rows | Key Characteristics |
|---|---|---|
| `customer_master.csv` | 10,000 | Credit scores correlated with income + employment |
| `loan_accounts.csv` | 18,000 | 5 product types; realistic 10-15% default rate |
| `repayments.csv` | 120,000 | DPD correlated to loan risk; missed = zero payment |
| `collections.csv` | 8,000 | 4 escalation levels; 8 action types |
| `defaults.csv` | 2,149 | NPL classification; recovery capped below outstanding |
| **Total** | **158,149** | |

---

## ✨ Key Features

### 🔄 SCD Type 2
Tracks `monthly_income`, `employment_status`, `customer_segment`, `credit_score` changes. Every change = new versioned row with `effective_date`, `expiry_date`, `is_current`, `version_number`.

### 📈 Incremental Loading
Repayments use `last_successful_load` from `audit.etl_control`. SSIS Conditional Split routes `NEW → bronze table` and `OLD → discard`.

### 🛡️ Data Quality Gates
5 automated DQ rules per batch. **Pipeline halts on FAIL.**

| Rule | Threshold | Severity |
|---|---|---|
| Null primary keys | 0% | FAIL |
| Orphan loan records | < 1% | FAIL |
| Balance > 130% of principal | < 0.5% | WARNING |
| Income out of range | < 2% | FAIL |
| Fact-to-Silver row gap | < 1% | FAIL |

### 💹 IFRS 9 Metrics per Fact Row

| Metric | Formula | Standard |
|---|---|---|
| PD | Logistic DPD approximation | Basel III IRB |
| LGD | Collateral-based floor rates (10–65%) | Basel III |
| EAD | Current outstanding balance | Basel III |
| ECL | PD × LGD × EAD | IFRS 9 |
| Stage | 1: <30DPD / 2: 30-89DPD / 3: 90+DPD | IFRS 9 |
---

## 📊 Analytics Views

| View | Purpose |
|---|---|
| `vw_portfolio_summary` | Monthly KPIs: exposure, NPL ratio, ECL coverage |
| `vw_delinquency_by_segment` | DPD buckets by customer segment × loan type |
| `vw_high_risk_customers` | Composite risk score — early intervention list |
| `vw_collections_effectiveness` | Collector performance, promise-to-pay rate |
| `vw_risk_migration` | Month-over-month IFRS9 stage transitions |
| `vw_portfolio_concentration` | Exposure % by province, segment, collateral |

---

## 📏 Regulatory Alignment

| Standard | Coverage |
|---|---|
| IFRS 9 | ECL staging, 12-month and lifetime ECL |
| Basel III | PD, LGD, EAD (IRB approach approximation) |
| SARB PS1 | NPL classification, 6% NPL ratio threshold |
| NCA (SA) | Responsible lending signals via income DQ checks |

---

## ✅ Test Results

```
41 passed in 2.14s
TestCustomerMaster        ::  10/10 passed
TestLoanAccounts          ::  10/10 passed
TestRepayments            ::   8/8  passed
TestCollections           ::   5/5  passed
TestDefaults              ::   5/5  passed
TestCrossDatasetIntegrity ::   3/3  passed
```

---

<div align="center">

**Author:Letsoalo M**

**Built as a production-grade portfolio project demonstrating advanced data engineering,**

**credit risk domain expertise, and end-to-end pipeline design.**
  
DISCLAIMER:THIS PRORECJT IS FOR EDUCATIONAL PURPOSE ONLY,NOT FINANCIAL ADVISE
