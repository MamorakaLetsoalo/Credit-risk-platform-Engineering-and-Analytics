-- =============================================================================
-- CREDIT RISK PLATFORM | SILVER LAYER
-- File:    04_silver_collections_table.sql
-- Layer:   Silver (Cleansed + Validated + Standardized)
-- Purpose: Type-cast, deduplicate, standardize, and validate bronze data.
--          Referential integrity enforced here.
-- =============================================================================

-- TABLE: silver.collections

IF OBJECT_ID('silver.collections', 'U') IS NOT NULL DROP TABLE silver.collections;
GO
CREATE TABLE silver.collections (
    collection_id        VARCHAR(20)         NOT NULL,
    loan_id              VARCHAR(20)         NOT NULL,
    collector_id         VARCHAR(20),
    action_date          DATE                NOT NULL,
    action_type          VARCHAR(50)         NOT NULL,
    outcome              VARCHAR(50),
    promised_amount      DECIMAL(18,2),
    next_action_date     DATE,
    escalation_level     VARCHAR(20),

--DQ
    dq_orphan_loan       BIT DEFAULT 0,

--lineage
    source_system        VARCHAR(50),
    batch_id             INT,
    silver_load_date     DATETIME2 DEFAULT GETDATE(),

    CONSTRAINT pk_silver_collection PRIMARY KEY (collection_id)
);