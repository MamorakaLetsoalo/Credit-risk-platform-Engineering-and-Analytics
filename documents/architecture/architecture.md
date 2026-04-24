# Credit Risk Platform — Architecture Document

**Version: 1.0 | Environment: PROD | Last Updated: 2024**


## 1. Overview

The Credit Risk & Loan Default Analytics Platform is a production-grade
data warehouse built on SQL Server using the Medallion Architecture pattern
(Bronze → Silver → Gold → Analytics). It ingests data from five core banking
source systems, applies IFRS 9 and Basel III risk metrics, and serves
executive and operational dashboards via Power BI / SSRS.

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                                │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐             │
│  │  Core Banking │  │    Loan      │  │  Collections  │             │
│  │  (Customers) │  │ Origination  │  │     CRM       │             │
│  └──────┬───────┘  └──────┬───────┘  └──────┬────────┘             │
│         │                 │                  │                       │
│  ┌──────┴─────────────────┴──────────────────┴──────────┐          │
│  │              CSV File Drop Zone                        │          │
│  │         C:\CreditRisk\data\source_files\              │          │
│  └───────────────────────────┬──────────────────────────┘          │
└───────────────────────────────┼─────────────────────────────────────┘
                                │ (nightly extract)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SSIS MASTER PACKAGE (Master_Credit_Risk_ETL.dtsx)      │
│                                                                      │
│  ┌──────────────────────┐                                           │
│  │  00_Initialize        │  Start batch, get last load datetime     │
│  └──────────┬───────────┘                                           │
│             ▼                                                        │
│  ┌──────────────────────┐                                           │
│  │  01_Bronze_Load       │  Bulk insert + incremental repayments    │
│  └──────────┬───────────┘                                           │
│             ▼                                                        │
│  ┌──────────────────────┐                                           │
│  │  02_Silver_Transform  │  Cleanse + DQ checks + MERGE upserts     │
│  └──────────┬───────────┘                                           │
│             ▼                                                        │
│  ┌──────────────────────┐                                           │
│  │  03_Gold_Risk_Model   │  SCD2 + dimensions + fact + ECL metrics  │
│  └──────────┬───────────┘                                           │
│             ▼                                                        │
│  ┌──────────────────────┐                                           │
│  │  04_Audit_Close       │  Close batch, row counts, DQ summary     │
│  └──────────────────────┘                                           │
│                                                                      │
│  [OnError] → error_log insert + failure email alert                 │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SQL SERVER: CreditRiskDB                          │
│                                                                      │
│  bronze.*    Raw ingestion tables (audit trail preserved forever)   │
│              └── customer_master, loan_accounts, repayments,        │
│                  collections, defaults                               │
│                                                                      │
│  silver.*    Cleansed + validated + typed tables                    │
│              └── customer_master, loan_accounts, repayments,        │
│                  collections, defaults                               │
│                                                                      │
│  gold.*      Star schema — analytical model                         │
│              ├── fact_credit_risk  (central fact)                   │
│              ├── dim_customer      (SCD Type 2)                     │
│              ├── dim_loan                                            │
│              ├── dim_date          (2015–2030 date spine)           │
│              ├── dim_repayment_status                               │
│              └── dim_default_reason                                 │
│                                                                      │
│  analytics.* Reporting views (VW)                                   │
│              ├── vw_portfolio_summary                               │
│              ├── vw_delinquency_by_segment                          │
│              ├── vw_high_risk_customers                             │
│              ├── vw_collections_effectiveness                       │
│              ├── vw_risk_migration                                  │
│              └── vw_portfolio_concentration                         │
│                                                                      │
│  audit.*     Pipeline observability                                  │
│              ├── etl_batch                                           │
│              ├── etl_control                                         │
│              ├── data_quality_log                                    │
│              └── error_log                                           │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     ANALYTICS LAYER                                  │
│                                                                      │
│  ┌───────────────────────┐    ┌──────────────────────────────┐     │
│  │  Power BI Dashboard   │    │  SSRS Regulatory Reports      │     │
│  │  ─────────────────── │    │  ─────────────────────────── │     │
│  │  • Portfolio Overview │    │  • NPL Report (SARB format)   │     │
│  │  • Delinquency Heat   │    │  • IFRS9 ECL Disclosure       │     │
│  │  • High Risk Alerts   │    │  • Provisions Schedule        │     │
│  │  • Risk Migration     │    │  • Collections Performance    │     │
│  └───────────────────────┘    └──────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   CI/CD PIPELINE (Azure DevOps)                      │
│                                                                      │
│  feature/* → develop → DEV (auto)                                   │
│  develop   → release/* → UAT (approval gate)                        │
│  release/* → main     → PROD (2 approvals + change record)          │
│                                                                      │
│  Pipeline: azure-pipelines.yml                                       │
│  Scripts:  cicd/deployment_strategy/deploy.ps1                      │
│  Tests:    cicd/deployment_strategy/smoke_tests.ps1                 │
└─────────────────────────────────────────────────────────────────────┘
```

## 3. Technology Stack

| Component         | Technology                    | Version  |
|-------------------|-------------------------------|----------|
| Database          | SQL Server                    | 2019+    |
| ETL Orchestration | SSIS (SQL Server IS)          | 2019     |
| Source Generation | Python (pandas, numpy, faker) | 3.11     |
| CI/CD             | Azure DevOps Pipelines        | Latest   |
| Source Control    | Git (Azure Repos)             | —        |
| Reporting         | Power BI + SSRS               | Latest   |
| Deployment        | PowerShell                    | 7.x      |

## 4. Data Flows

### Full Load (First Run)
1. SSIS reads all 5 CSV files
2. All records bulk-inserted to Bronze
3. Silver MERGE processes all records
4. Gold dimension rebuild + full fact insert

### Incremental Load (Daily)
1. SSIS reads `audit.etl_control.last_successful_load` for repayments
2. Repayments: Conditional Split — only new records inserted
3. Customers/Loans: MERGE upsert (handles inserts + updates)
4. Gold: SCD2 applied — changed customers get new version row
5. Fact: Only new repayment records processed

## 5. Scheduling

| Job                              | Schedule        | SQL Agent Job Name        |
|----------------------------------|-----------------|---------------------------|
| Master ETL (full pipeline)       | Daily 02:00 AM  | CreditRisk_Master_ETL     |
| DQ Report email                  | Daily 07:00 AM  | CreditRisk_DQ_Report      |
| Portfolio summary refresh        | Daily 06:00 AM  | CreditRisk_Analytics_Refresh|
| Archive old bronze records       | Monthly 1st     | CreditRisk_Bronze_Archive |

## 6. Security

- Bronze layer: ETL service account only (write)
- Silver layer: ETL service account (write) + Data Engineers (read)
- Gold layer: ETL service account (write) + Analysts (read)
- Analytics views: All authenticated AD users in Risk group (read)
- audit schema: ETL service account only

## 7. Disaster Recovery

| Component    | RPO    | RTO    | Strategy                            |
|--------------|--------|--------|-------------------------------------|
| SQL Database | 1 hour | 4 hours| SQL Always On AG + hourly log backup|
| Source CSVs  | 24 hrs | 2 hours| Azure Blob Storage (SFTP mirror)    |
| SSIS Packages| N/A    | 30 min | Git + Azure DevOps (redeploy)       |

## 8. Row Count Estimates (Production Scale)

| Table                      | Est. Rows (Year 1) | Growth Rate    |
|----------------------------|--------------------|----------------|
| bronze.repayments          | 1.5M+              | ~120K/month    |
| silver.repayments          | 1.4M+              | ~115K/month    |
| gold.fact_credit_risk      | 1.4M+              | ~115K/month    |
| gold.dim_customer (all ver)| 50K+               | ~2K/month      |
| gold.dim_loan              | 18K+               | ~1.5K/month    |
