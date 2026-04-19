-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    01_gold_star_schema.sql
-- Purpose:  star schema —  dimensions.
--          This is the analytical layer consumed by dashboards and reports.
-- =============================================================================

-- DIM: gold.dim_customer  ← SCD TYPE 2
-- Tracks historical changes in income, employment, segment

IF OBJECT_ID('gold.dim_customer', 'U') IS NOT NULL DROP TABLE gold.dim_customer;
GO
CREATE TABLE gold.dim_customer (
    customer_key         INT IDENTITY(1,1)   NOT NULL,

    -- Business key
    customer_id          VARCHAR(20)         NOT NULL,

    -- Tracked attributes (SCD2)
    customer_name        VARCHAR(200),
    id_number            VARCHAR(20),
    date_of_birth        DATE,
    gender               CHAR(1),
    employment_status    VARCHAR(50),
    monthly_income       DECIMAL(18,2),
    marital_status       VARCHAR(30),
    customer_segment     VARCHAR(50),
    province             VARCHAR(50),
    credit_score         SMALLINT,
    onboarding_date      DATE,

    -- Derived risk bands (computed from income + credit score)
    income_band          VARCHAR(20),        -- Low / Medium / High / Premium
    credit_risk_band     VARCHAR(20),        -- Very Low / Low / Medium / High / Very High

    -- SCD2 versioning columns
    effective_date       DATE                NOT NULL,
    expiry_date          DATE                NOT NULL DEFAULT '9999-12-31',
    is_current           BIT                 NOT NULL DEFAULT 1,
    version_number       INT                 NOT NULL DEFAULT 1,

    -- Lineage
    batch_id             INT,
    gold_load_date       DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_key),
    INDEX idx_dim_customer_biz (customer_id, is_current)
);

SELECT * FROM [gold].[dim_customer];
