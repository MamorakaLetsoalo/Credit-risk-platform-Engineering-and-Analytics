-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    01_gold_star_schema.sql
-- Purpose:  star schema — fact table 
--          This is the analytical layer consumed by dashboards and reports.
-- =============================================================================

-- FACT TABLE: gold.fact_credit_risk
-- Grain: one row per loan per repayment period

IF OBJECT_ID('gold.fact_credit_risk', 'U') IS NOT NULL DROP TABLE gold.fact_credit_risk;
GO
CREATE TABLE gold.fact_credit_risk (
    risk_fact_id                 BIGINT IDENTITY(1,1)    NOT NULL,

    -- Foreign keys to dimensions
    customer_key                 INT                     NOT NULL,
    loan_key                     INT                     NOT NULL,
    repayment_key                INT                     NOT NULL,
    date_key                     INT                     NOT NULL,   -- payment date
    due_date_key                 INT,

    -- Degenerate dimensions (kept on fact for drill-through)
    repayment_id                 VARCHAR(20),

    -- Additive measures
    exposure_amount              DECIMAL(38,2),           -- current_balance at time of payment
    payment_amount               DECIMAL(38,2),
    expected_amount              DECIMAL(38,2),
    payment_shortfall            DECIMAL(38,2),           -- expected - paid

    -- Risk indicators
    days_past_due                INT,
    default_flag                 BIT                     DEFAULT 0,
    npl_flag                     BIT                     DEFAULT 0,  -- Non-Performing Loan

    -- Basel / IFRS 9 risk metrics
    probability_of_default       DECIMAL(38,2),            -- PD
    loss_given_default           DECIMAL(38,2),            -- LGD (regulatory estimate)
    exposure_at_default          DECIMAL(38,2),           -- EAD
    expected_credit_loss         DECIMAL(38,2),           -- ECL = PD × LGD × EAD
    ecl_stage                    TINYINT,                 -- IFRS9: 1=Performing, 2=Underperforming, 3=Credit-Impaired

    -- Derived flags
    dpd_bucket                   VARCHAR(20),             -- Current / 1-30 / 31-60 / 61-90 / 90+
    is_collections_active        BIT                     DEFAULT 0,

    -- Lineage
    batch_id                     INT,
    gold_load_date               DATETIME2               DEFAULT GETDATE(),

    CONSTRAINT pk_fact_credit_risk PRIMARY KEY (risk_fact_id),
    CONSTRAINT fk_fact_customer    FOREIGN KEY (customer_key)  REFERENCES gold.dim_customer(customer_key),
    CONSTRAINT fk_fact_loan        FOREIGN KEY (loan_key)      REFERENCES gold.dim_loan(loan_key),
    CONSTRAINT fk_fact_repayment   FOREIGN KEY (repayment_key) REFERENCES gold.dim_repayment_status(repayment_key),
    CONSTRAINT fk_fact_date        FOREIGN KEY (date_key)      REFERENCES gold.dim_date(date_key),

    INDEX idx_fact_customer   (customer_key),
    INDEX idx_fact_loan       (loan_key),
    INDEX idx_fact_date       (date_key),
    INDEX idx_fact_dpd        (days_past_due, default_flag),
    INDEX idx_fact_ecl_stage  (ecl_stage)
);