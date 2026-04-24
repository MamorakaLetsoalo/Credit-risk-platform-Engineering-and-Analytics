# Credit Risk Platform — Business Rules
# =============================================================================
# All credit risk business rules applied in the Silver and Gold layers.
# Every rule here maps to a DQ check or a transformation in the SQL scripts.
# =============================================================================

## 1. Data Ingestion Rules (Bronze Layer)

| Rule ID | Rule                                            | Action on Failure        |
|---------|-------------------------------------------------|--------------------------|
| BR-001  | customer_id must be non-null                    | Reject row to error_log  |
| BR-002  | loan_id must be non-null                        | Reject row               |
| BR-003  | repayment_id must be non-null                   | Reject row               |
| BR-004  | Duplicate records flagged (is_duplicate_flag=1) | Exclude from Silver load |
| BR-005  | All monetary values must be parseable as DECIMAL| Reject row if unparseable|

## 2. Data Quality Thresholds (Silver Layer)

| Rule ID | Check                            | Max Failure % | Severity |
|---------|----------------------------------|---------------|----------|
| DQ-001  | Null customer_id in Bronze       | 0.00%         | FAIL     |
| DQ-002  | Orphan loans (no customer match) | 1.00%         | FAIL     |
| DQ-003  | Balance > 130% of principal      | 0.50%         | WARNING  |
| DQ-004  | Income out of range              | 2.00%         | FAIL     |
| DQ-005  | Future payment dates             | 0.10%         | WARNING  |
| DQ-006  | Fact row completeness vs Silver  | 1.00%         | FAIL     |

## 3. Classification Rules

### Income Bands
| Band    | Monthly Income (ZAR)   |
|---------|------------------------|
| Low     | < R10,000              |
| Medium  | R10,000 – R29,999      |
| High    | R30,000 – R79,999      |
| Premium | ≥ R80,000              |

### Credit Score → Risk Band
| Risk Band  | Credit Score Range |
|------------|--------------------|
| Very Low   | 750 – 850          |
| Low        | 680 – 749          |
| Medium     | 600 – 679          |
| High       | 500 – 599          |
| Very High  | 300 – 499          |

### Loan Size Bands
| Band   | Loan Amount (ZAR)         |
|--------|---------------------------|
| Micro  | < R10,000                 |
| Small  | R10,000 – R99,999         |
| Medium | R100,000 – R499,999       |
| Large  | R500,000 – R1,999,999     |
| Mega   | ≥ R2,000,000              |

## 4. IFRS 9 Staging Rules

| Stage | Label           | Criteria                                              |
|-------|-----------------|-------------------------------------------------------|
| 1     | Performing      | DPD < 30 and no significant increase in credit risk   |
| 2     | Under-performing| DPD 30–89 OR significant increase in credit risk      |
| 3     | Credit-Impaired | DPD ≥ 90 OR loan in Default / Written-Off status      |

ECL Measurement:
- Stage 1: 12-month ECL
- Stage 2: Lifetime ECL
- Stage 3: Lifetime ECL (impaired)

## 5. PD Calculation Rules (Simplified Logistic Approximation)

| DPD Range   | Probability of Default |
|-------------|------------------------|
| 0 days      | 0.50%                  |
| 1–30 days   | 5% + (DPD/30 × 10%)    |
| 31–90 days  | 15% + (DPD/90 × 35%)   |
| 91–180 days | 50% + (DPD/180 × 30%)  |
| > 180 days  | 99%                    |

> Production note: Replace with IRB model or credit bureau PD scores when available.

## 6. LGD Rules (Basel III Floor Rates)

| Collateral Type | LGD  |
|-----------------|------|
| Property        | 35%  |
| Vehicle         | 40%  |
| Cash Deposit    | 10%  |
| Surety          | 45%  |
| None (unsecured)| 65%  |

## 7. SCD Type 2 Trigger Rules

A new customer dimension version is created when any of the following change:

| Attribute         | Business Justification                              |
|-------------------|-----------------------------------------------------|
| monthly_income    | Income change directly affects credit risk profile  |
| employment_status | Employment loss is a leading default indicator      |
| customer_segment  | Segment migration affects product eligibility       |
| credit_score      | Score change reflects updated creditworthiness      |

Non-tracked (updated in-place): email, phone, marital_status.

## 8. Non-Performing Loan (NPL) Classification

A loan is classified NPL when:
- days_past_due >= 90, OR
- loan_status = 'Default', OR
- loan_status = 'Written-Off'

NPL ratio = Total NPL Exposure / Total Loan Book Exposure × 100

SARB regulatory threshold: NPL ratio must not exceed 6% for retail books.

## 9. Collections Escalation Rules

| DPD Range | Escalation Level | Primary Action        |
|-----------|------------------|-----------------------|
| 1–30      | Level 1          | SMS + Outbound Call   |
| 31–60     | Level 2          | Letter + Field Visit  |
| 61–90     | Level 3          | Legal Notice          |
| 90+       | Legal            | Legal proceedings     |

## 10. Write-Off Policy

A loan is eligible for write-off when:
- DPD > 180 days AND
- Recovery attempts exhausted (Level 3 + Legal actions completed) AND
- Recovery amount < 10% of outstanding balance

Write-offs must be approved by: Credit Risk Manager + CFO sign-off.
