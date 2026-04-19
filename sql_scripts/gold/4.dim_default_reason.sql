-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    01_gold_star_schema.sql
-- Purpose:  star schema —  dimensions.
--          This is the analytical layer consumed by dashboards and reports.
-- =============================================================================

-- DIM: gold.dim_default_reason
-- ─────────────────────────────────────────────────────────────────────────────
IF OBJECT_ID('gold.dim_default_reason', 'U') IS NOT NULL DROP TABLE gold.dim_default_reason;
GO
CREATE TABLE gold.dim_default_reason (
    default_reason_key   INT IDENTITY(1,1)   NOT NULL,
    default_reason       VARCHAR(100)        NOT NULL,
    reason_category      VARCHAR(50),        -- Economic / Personal / Fraud / Unknown
    is_preventable       BIT,

    CONSTRAINT pk_dim_default_reason PRIMARY KEY (default_reason_key)
);
GO
INSERT INTO gold.dim_default_reason (default_reason, reason_category, is_preventable)
VALUES
    ('Job Loss',             'Economic',  1),
    ('Business Failure',     'Economic',  1),
    ('Medical Emergency',    'Personal',  0),
    ('Fraud',                'Fraud',     1),
    ('Divorce',              'Personal',  0),
    ('Death',                'Personal',  0),
    ('Overcommitment',       'Personal',  1),
    ('Economic Downturn',    'Economic',  0),
    ('Unknown',              'Unknown',   0);
