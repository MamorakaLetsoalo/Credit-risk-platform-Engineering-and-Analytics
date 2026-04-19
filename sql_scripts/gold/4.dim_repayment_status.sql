-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    01_gold_star_schema.sql
-- Purpose:  star schema —  dimensions.
--          This is the analytical layer consumed by dashboards and reports.
-- =============================================================================

-- DIM: gold.dim_repayment_status

IF OBJECT_ID('gold.dim_repayment_status', 'U') IS NOT NULL DROP TABLE gold.dim_repayment_status;
GO
CREATE TABLE gold.dim_repayment_status (
    repayment_key        INT IDENTITY(1,1)   NOT NULL,
    repayment_status     VARCHAR(50)         NOT NULL,
    status_category      VARCHAR(50),        -- On Time / Delinquent / Defaulted
    dpd_bucket           VARCHAR(50),        -- Current / 1-30 DPD / 31-60 / 61-90 / 90+
    risk_weight          DECIMAL(38,2),       -- regulatory risk weight

    CONSTRAINT pk_dim_repayment_status PRIMARY KEY (repayment_key)
);
GO
-- Seed the lookup
INSERT INTO gold.dim_repayment_status (repayment_status, status_category, dpd_bucket, risk_weight)
VALUES
    ('PAID ON TIME',    'On Time',    'Current', 0.00),
    ('PAID LATE',       'Delinquent', '1-30 DPD', 0.25),
    ('LATE',            'Delinquent', '31-90 DPD', 0.50),
    ('MISSED',          'Defaulted',  '90+ DPD', 1.00),
    ('RESTRUCTURED',    'Delinquent', '1-30 DPD', 0.35),
    ('UNKNOWN',         'Unknown',    'Unknown', 0.50);
GO

select * from [gold].[dim_repayment_status];