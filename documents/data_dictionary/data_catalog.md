# Credit Risk Platform — Data Dictionary


**Complete column-level documentation for all layers.**

**Update this file whenever a column is added, renamed, or removed.**


## Bronze Layer

### bronze.customer_master
| Column               | Type         | Description                               | Source Field      |
|----------------------|--------------|-------------------------------------------|-------------------|
| customer_id          | VARCHAR(20)  | Unique customer identifier (CUST######)   | customer_id       |
| customer_name        | VARCHAR(200) | Full name as per ID document              | customer_name     |
| id_number            | VARCHAR(20)  | National ID / Passport number             | id_number         |
| date_of_birth        | VARCHAR(20)  | Raw DOB string — parsed in Silver         | date_of_birth     |
| gender               | VARCHAR(10)  | M / F / Other                             | gender            |
| employment_status    | VARCHAR(50)  | Employed / Self-Employed / Unemployed etc | employment_status |
| monthly_income       | VARCHAR(30)  | Gross monthly income in ZAR               | monthly_income    |
| marital_status       | VARCHAR(30)  | Married / Single / Divorced / Widowed     | marital_status    |
| customer_segment     | VARCHAR(50)  | Retail / Business / Private Banking etc   | customer_segment  |
| province             | VARCHAR(50)  | South African province                    | province          |
| credit_score         | VARCHAR(10)  | Credit bureau score (300-850)             | credit_score      |
| onboarding_date      | VARCHAR(20)  | Date customer relationship started        | onboarding_date   |
| load_date            | DATETIME2    | Timestamp this row was ingested           | Pipeline          |
| batch_id             | INT          | Links to audit.etl_batch                  | Pipeline          |
| is_duplicate_flag    | BIT          | 1 = duplicate of another row in batch     | Pipeline          |

### bronze.loan_accounts
| Column               | Type         | Description                                        |
|----------------------|--------------|----------------------------------------------------|
| loan_id              | VARCHAR(20)  | Unique loan identifier (LOAN#######)               |
| customer_id          | VARCHAR(20)  | FK to customer_master                              |
| loan_type            | VARCHAR(50)  | Home Loan / Personal Loan / Vehicle Finance etc    |
| loan_amount          | VARCHAR(30)  | Original principal amount in ZAR                   |
| interest_rate        | VARCHAR(15)  | Annual interest rate (%)                           |
| loan_term_months     | VARCHAR(10)  | Agreed repayment term in months                    |
| loan_start_date      | VARCHAR(20)  | Date loan was disbursed                            |
| loan_end_date        | VARCHAR(20)  | Scheduled maturity date                            |
| current_balance      | VARCHAR(30)  | Outstanding balance as of extract date             |
| loan_status          | VARCHAR(30)  | Active / Closed / Default / Written-Off            |
| collateral_type      | VARCHAR(50)  | Property / Vehicle / None / Cash Deposit / Surety  |

### bronze.repayments
| Column               | Type         | Description                                        |
|----------------------|--------------|----------------------------------------------------|
| repayment_id         | VARCHAR(20)  | Unique repayment reference (REP########)           |
| loan_id              | VARCHAR(20)  | FK to loan_accounts                                |
| payment_date         | VARCHAR(20)  | Date payment was received                          |
| due_date             | VARCHAR(20)  | Scheduled payment due date                         |
| payment_amount       | VARCHAR(30)  | Amount actually paid in ZAR                        |
| expected_amount      | VARCHAR(30)  | Scheduled instalment amount                        |
| days_past_due        | VARCHAR(10)  | Days between due_date and payment_date             |
| repayment_status     | VARCHAR(30)  | Paid On Time / Paid Late / Late / Missed           |
| payment_method       | VARCHAR(50)  | EFT / Debit Order / Cash / Online Transfer         |

---

## Silver Layer

### silver.customer_master (differences from Bronze)
| Column            | Type          | Notes                                                       |
|-------------------|---------------|-------------------------------------------------------------|
| monthly_income    | DECIMAL(18,2) | Parsed and validated. Negative values converted to positive |
| credit_score      | SMALLINT      | Validated range 300-850                                     |
| gender            | CHAR(1)       | Standardised to M / F / U                                   |
| dq_income_flag    | BIT           | 1 = income outside valid range (0 to 5,000,000)             |
| dq_email_flag     | BIT           | 1 = email does not match x@y.z pattern                      |
| dq_dob_flag       | BIT           | 1 = DOB is in the future or before 1900-01-01               |

### silver.loan_accounts (computed columns)
| Column                    | Type            | Formula                                  |
|---------------------------|-----------------|------------------------------------------|
| loan_to_value_ratio       | DECIMAL (COMP.) | current_balance / loan_amount            |
| dq_orphan_customer        | BIT             | 1 = no silver.customer_master match      |
| dq_balance_exceeds_amount | BIT             | 1 = balance > 130% of original amount    |
| dq_date_mismatch          | BIT             | 1 = loan_start_date >= loan_end_date     |

### silver.repayments (computed columns)
| Column            | Type            | Formula                                        |
|-------------------|-----------------|------------------------------------------------|
| is_late           | BIT (COMP.)     | days_past_due > 0                              |
| payment_shortfall | DECIMAL (COMP.) | expected_amount - payment_amount               |

---

## Gold Layer — Fact Table

### gold.fact_credit_risk
| Column                   | Type          | Description                                           |
|--------------------------|---------------|-------------------------------------------------------|
| risk_fact_id             | BIGINT        | Surrogate PK                                          |
| customer_key             | INT           | FK to dim_customer (current version)                  |
| loan_key                 | INT           | FK to dim_loan                                        |
| repayment_key            | INT           | FK to dim_repayment_status                            |
| date_key                 | INT           | FK to dim_date (YYYYMMDD)                             |
| exposure_amount          | DECIMAL(18,2) | Outstanding balance at time of repayment              |
| payment_shortfall        | DECIMAL(18,2) | Expected minus actual payment                         |
| days_past_due            | INT           | DPD as of payment date                                |
| default_flag             | BIT           | 1 = loan has Default status                           |
| npl_flag                 | BIT           | 1 = days_past_due >= 90 (Non-Performing Loan)         |
| probability_of_default   | DECIMAL(7,4)  | PD estimate (logistic approx from DPD; 0.005 to 0.99) |
| loss_given_default       | DECIMAL(7,4)  | LGD by collateral type (Basel floor rates)            |
| exposure_at_default      | DECIMAL(18,2) | EAD = current outstanding balance                     |
| expected_credit_loss     | DECIMAL(18,2) | ECL = PD × LGD × EAD (IFRS 9)                        |
| ecl_stage                | TINYINT       | 1=Performing (<30DPD), 2=Under-performing, 3=Impaired |
| dpd_bucket               | VARCHAR(20)   | Current / 1-30 DPD / 31-60 / 61-90 / 90+             |
| is_collections_active    | BIT           | 1 = collections activity in past 90 days              |

---

## Gold Layer — Dimensions

### gold.dim_customer (SCD Type 2 tracking columns)
| Column            | Description                                                   |
|-------------------|---------------------------------------------------------------|
| customer_key      | Surrogate PK — never reused                                   |
| customer_id       | Business key from source                                      |
| income_band       | Low / Medium / High / Premium (derived from monthly_income)   |
| credit_risk_band  | Very Low / Low / Medium / High / Very High (from credit_score) |
| effective_date    | Date this version of the record became active                 |
| expiry_date       | Date this version was superseded (9999-12-31 = current)       |
| is_current        | 1 = latest version of this customer                           |
| version_number    | 1 = original, 2+ = changed versions                          |

**Tracked attributes (trigger new SCD2 version):**
- monthly_income
- employment_status
- customer_segment
- credit_score

**Non-tracked attributes (updated in place):**
- email, phone, marital_status (point-in-time not business-critical)

---

## Risk Metrics Reference

| Metric | Formula                       | Regulatory Reference        |
|--------|-------------------------------|-----------------------------|
| PD     | Logistic approx from DPD      | Basel III IRB Approach      |
| LGD    | Collateral-based floor rate   | Basel III (35-65% range)    |
| EAD    | Current outstanding balance   | Basel III                   |
| ECL    | PD × LGD × EAD               | IFRS 9 (IASB)               |
| NPL    | DPD >= 90 days                | SARB Prudential Standard 1  |
| NPA    | DPD >= 90 + no repayment      | SARB Prudential Standard 1  |

## DPD Classification (South African Banking)
| Bucket    | DPD Range | IFRS9 Stage | Action Required              |
|-----------|-----------|-------------|------------------------------|
| Current   | 0 days    | Stage 1     | Routine monitoring           |
| 1-30 DPD  | 1-30      | Stage 1     | Early warning flag           |
| 31-60 DPD | 31-60     | Stage 2     | Collections contact          |
| 61-90 DPD | 61-90     | Stage 2     | Escalate to senior collector |
| 90+ DPD   | 91+       | Stage 3     | Legal / Write-off review     |
