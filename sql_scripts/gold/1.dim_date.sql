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

-- Populate date dimension
;WITH date_cte AS (
    SELECT CAST('2015-01-01' AS DATE) AS d
    UNION ALL
    SELECT DATEADD(DAY, 1, d) FROM date_cte WHERE d < '2030-12-31'
)
INSERT INTO gold.dim_date
SELECT
    CAST(FORMAT(d, 'yyyyMMdd') AS INT)          AS date_key,
    d                                            AS full_date,
    DATEPART(WEEKDAY, d)                         AS day_of_week,
    DATENAME(WEEKDAY, d)                         AS day_name,
    DAY(d)                                       AS day_of_month,
    DATEPART(DAYOFYEAR, d)                       AS day_of_year,
    DATEPART(WEEK, d)                            AS week_of_year,
    MONTH(d)                                     AS month_number,
    DATENAME(MONTH, d)                           AS month_name,
    LEFT(DATENAME(MONTH, d), 3)                  AS month_short,
    DATEPART(QUARTER, d)                         AS quarter_number,
    CONCAT('Q', DATEPART(QUARTER, d))            AS quarter_name,
    YEAR(d)                                      AS year_number,
    CASE WHEN DATEPART(WEEKDAY,d) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN d = EOMONTH(d) THEN 1 ELSE 0 END  AS is_month_end,
    CASE WHEN d = EOMONTH(d) AND MONTH(d) IN (3,6,9,12) THEN 1 ELSE 0 END AS is_quarter_end,
    CASE WHEN MONTH(d)=12 AND DAY(d)=31 THEN 1 ELSE 0 END AS is_year_end,
    -- SA Fiscal Year: April - March
    CASE WHEN MONTH(d) >= 4 THEN YEAR(d) ELSE YEAR(d)-1 END AS fiscal_year,
    CASE WHEN MONTH(d) IN (4,5,6) THEN 1
         WHEN MONTH(d) IN (7,8,9) THEN 2
         WHEN MONTH(d) IN (10,11,12) THEN 3
         ELSE 4 END                              AS fiscal_quarter,
    CASE WHEN MONTH(d) >= 4 THEN MONTH(d)-3 ELSE MONTH(d)+9 END AS fiscal_month
FROM date_cte
OPTION (MAXRECURSION 0);