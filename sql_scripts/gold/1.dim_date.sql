-- =============================================================================
-- CREDIT RISK PLATFORM | GOLD LAYER
-- File:    01_gold_star_schema.sql
-- Purpose:  star schema —  dimensions.
--          This is the analytical layer consumed by dashboards and reports.
-- =============================================================================


-- DIM: gold.dim_date
-- Pre-populated date spine: 2015-01-01 to 2030-12-31

IF OBJECT_ID('gold.dim_date', 'U') IS NOT NULL DROP TABLE gold.dim_date;
GO
CREATE TABLE gold.dim_date (
    date_key            INT             NOT NULL,   -- YYYYMMDD
    full_date           DATE            NOT NULL,
    day_of_week         TINYINT,                    -- 1=Sun, 7=Sat
    day_name            VARCHAR(10),
    day_of_month        TINYINT,
    day_of_year         SMALLINT,
    week_of_year        TINYINT,
    month_number        TINYINT,
    month_name          VARCHAR(10),
    month_short         CHAR(3),
    quarter_number      TINYINT,
    quarter_name        VARCHAR(6),
    year_number         SMALLINT,
    is_weekend          BIT,
    is_month_end        BIT,
    is_quarter_end      BIT,
    is_year_end         BIT,
    fiscal_year         SMALLINT,                   -- SA fiscal: April-March
    fiscal_quarter      TINYINT,
    fiscal_month        TINYINT,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);