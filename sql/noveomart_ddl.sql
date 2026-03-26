-- =============================================================================
-- NoveoMart Finance Reconciliation Platform
-- Snowflake DDL — All 16 Tables
-- File: sql/noveomart_ddl.sql
--
-- Run order: Dimensions first, then Facts, then Derived table last.
-- All tables live in: noveomart_db.raw
--
-- Usage:
--   1. Run this script in a Snowflake Worksheet
--   2. Upload CSVs via SnowSQL PUT command
--   3. Load each table with COPY INTO (examples at bottom of file)
--
-- Conventions:
--   • country_code on every table — universal partition key
--   • VARCHAR lengths match generator output exactly
--   • _dq_flag VARCHAR on transactional tables — 'CLEAN' or issue code
--   • All monetary amounts as NUMBER(18,2)
--   • All rates/percentages as NUMBER(8,4)
--   • All boolean flags as BOOLEAN (TRUE/FALSE) — standardised from VARCHAR(1)/NUMBER(1)
--   • Audit trail on all fact + derived tables: created_at, updated_at, loaded_by
--
-- NOTE: 
-- CREATE OR REPLACE used intentionally for initial/reset loads in this portfolio environment. 
-- In production, this would be CREATE TABLE IF NOT EXISTS + ALTER TABLE for schema evolution,
-- with careful handling of changes to existing columns.
-- migrations managed via dbt or Flyway.
-- =============================================================================

USE DATABASE noveomart_db;
USE SCHEMA raw;
USE WAREHOUSE portfolio_compute_wh;


-- =============================================================================
-- SECTION 1 — DIMENSION TABLES (run first, no dependencies)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1.1  DIM_COUNTRY
-- Configuration brain. Every country-specific setting lives here.
-- Thresholds, tax system, payment rails, regulatory body.
-- Adding a new country = one INSERT here, zero schema changes elsewhere.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_country (
    country_code            VARCHAR(3)      NOT NULL,   -- ISO 3166-1 alpha-3: MYS, SGP
    country_code_iso2       VARCHAR(2),                 -- ISO 3166-1 alpha-2: MY, SG
    country_name            VARCHAR(50),
    sap_company_code        VARCHAR(4),                 -- SAP: MY01, SG01
    base_currency           VARCHAR(3),                 -- MYR, SGD
    group_currency          VARCHAR(3),                 -- USD (group reporting)
    tax_system              VARCHAR(10),                -- SST, GST
    tax_rate_standard       NUMBER(8,4),                -- 6.0 (MY SST), 9.0 (SG GST)
    fiscal_year_end_month   NUMBER(2),                  -- 12 for Dec, 6 for Jun (AU)
    timezone                VARCHAR(10),                -- UTC+8
    settlement_lag_days     NUMBER(2),                  -- Expected T+N settlement
    variance_threshold_t2   NUMBER(18,2),               -- Auto-clear (TOLERANCE) threshold
    variance_threshold_t3   NUMBER(18,2),               -- Escalate to Finance manager
    variance_threshold_t4   NUMBER(18,2),               -- Escalate to IT + Controller
    regulatory_body         VARCHAR(50),                -- BNM (MY), MAS (SG)
    data_privacy_law        VARCHAR(100),
    dominant_ewallet        VARCHAR(200),               -- Comma-separated: TnG, GrabPay...
    bank_transfer_rail      VARCHAR(50),                -- IBG / DuitNow (MY), FAST (SG)
    is_active               BOOLEAN         DEFAULT TRUE

    , CONSTRAINT pk_dim_country PRIMARY KEY (country_code)
);


-- -----------------------------------------------------------------------------
-- 1.2  DIM_STORE
-- Store master data. 15 stores: 8 Malaysia, 7 Singapore.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_store (
    store_id                VARCHAR(20)     NOT NULL,   -- MY-STR-001, SG-STR-001
    country_code            VARCHAR(3)      NOT NULL,
    store_name              VARCHAR(100),               -- NoveoMart KLCC
    mall_name               VARCHAR(100),               -- Suria KLCC
    city                    VARCHAR(50),
    state                   VARCHAR(50),
    store_type              VARCHAR(20),                -- FLAGSHIP, STANDARD
    sap_profit_center       VARCHAR(20),                -- PC-MY-001
    sap_cost_center         VARCHAR(20),                -- CC-MY-001
    bank_account            VARCHAR(50),                -- Store settlement bank account
    bank_code               VARCHAR(10),                -- MAYB, OCBC
    mas_merchant_id         VARCHAR(50),                -- Card scheme merchant ID
    tax_registration        VARCHAR(50),                -- SST/GST registration number
    store_manager           VARCHAR(100),
    open_date               DATE,
    is_active               BOOLEAN         DEFAULT TRUE,
    floor_area_sqft         NUMBER(10,0),
    num_terminals           NUMBER(3,0)

    , CONSTRAINT pk_dim_store PRIMARY KEY (store_id)
    , CONSTRAINT fk_dim_store_country FOREIGN KEY (country_code)
        REFERENCES dim_country(country_code)
);


-- -----------------------------------------------------------------------------
-- 1.3  DIM_TENDER_TYPE
-- Payment method master. MDR rates, settlement lag by tender.
-- country_code = 'BOTH' means valid for SG and MY.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_tender_type (
    tender_code             VARCHAR(20)     NOT NULL,   -- CASH, VISA, TNG, PAYNOW...
    country_code            VARCHAR(5)      NOT NULL,   -- MYS, SGP, or BOTH
    tender_name             VARCHAR(50),
    tender_category         VARCHAR(20),                -- CASH, CARD, EWALLET, BNPL
    sap_gl_account          VARCHAR(20),                -- GL account for clearing
    settlement_lag_days     NUMBER(2),                  -- 0=same day, 1=T+1, 3=T+3
    is_card                 BOOLEAN         DEFAULT FALSE,
    is_ewallet              BOOLEAN         DEFAULT FALSE,
    interchange_rate_pct    NUMBER(8,4),                -- Contracted MDR %
    is_active               BOOLEAN         DEFAULT TRUE,
    notes                   VARCHAR(500)

    , CONSTRAINT pk_dim_tender PRIMARY KEY (tender_code, country_code)
);


-- -----------------------------------------------------------------------------
-- 1.4  DIM_BANK
-- Bank account master. One row per bank account used for settlement.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_bank (
    bank_code               VARCHAR(10)     NOT NULL,   -- MAYB, OCBC, DBS, UOB...
    country_code            VARCHAR(3)      NOT NULL,
    bank_name               VARCHAR(100),
    bank_short_name         VARCHAR(20),
    swift_bic               VARCHAR(15),
    bank_account_no         VARCHAR(50),
    account_type            VARCHAR(20),                -- COLLECTION, OPERATING
    sap_house_bank          VARCHAR(10),                -- SAP house bank key
    sap_account_id          VARCHAR(10),                -- SAP account ID
    statement_format        VARCHAR(10),                -- MT940, CSV
    transfer_rail           VARCHAR(30),                -- IBG / DuitNow, FAST, RTGS
    settlement_cut_off      VARCHAR(10),                -- 17:00
    contact_name            VARCHAR(100),
    is_active               BOOLEAN         DEFAULT TRUE

    , CONSTRAINT pk_dim_bank PRIMARY KEY (bank_code, country_code)
);


-- -----------------------------------------------------------------------------
-- 1.5  DIM_TAX_CODE
-- SST codes (Malaysia) and GST codes (Singapore).
-- Links to SAP tax code for GL account validation.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_tax_code (
    tax_code_key            VARCHAR(20)     NOT NULL,   -- MYS-SR-6, SGP-SR-9
    country_code            VARCHAR(3)      NOT NULL,
    tax_code                VARCHAR(10),                -- SR-6, TX9
    tax_type                VARCHAR(5),                 -- SST, GST
    tax_description         VARCHAR(100),
    tax_rate_pct            NUMBER(8,4),                -- 6.0, 9.0
    effective_date          DATE,
    end_date                DATE,
    sap_tax_code            VARCHAR(5),                 -- SAP: S6, TX
    is_exempt               BOOLEAN         DEFAULT FALSE,
    gl_account_tax          VARCHAR(20),                -- Tax payable GL
    filing_frequency        VARCHAR(20),                -- Monthly, Bi-monthly, Quarterly
    tax_authority           VARCHAR(100)

    , CONSTRAINT pk_dim_tax_code PRIMARY KEY (tax_code_key)
);


-- -----------------------------------------------------------------------------
-- 1.6  DIM_CURRENCY
-- Daily FX rates Oct 2024 – Mar 2025.
-- Covers MYR and SGD to USD and cross-rates.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_currency (
    currency_code           VARCHAR(3)      NOT NULL,
    currency_name           VARCHAR(50),
    currency_symbol         VARCHAR(5),
    decimal_places          NUMBER(1),
    rate_date               DATE            NOT NULL,
    rate_to_usd             NUMBER(18,6),
    rate_to_sgd             NUMBER(18,6),
    rate_source             VARCHAR(20),                -- BNM, MAS, ECB
    is_group_currency       BOOLEAN         DEFAULT FALSE

    , CONSTRAINT pk_dim_currency PRIMARY KEY (currency_code, rate_date)
);


-- -----------------------------------------------------------------------------
-- 1.7  DIM_FISCAL_CALENDAR
-- Working days, public holidays, period-close flags by country.
-- Drives the period-end risk flag on Page 5 trend chart.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_fiscal_calendar (
    fiscal_period_key       VARCHAR(15)     NOT NULL,   -- MYS-2024-10, SGP-2024-11
    country_code            VARCHAR(3)      NOT NULL,
    date_key                DATE            NOT NULL,
    fiscal_year             NUMBER(4),
    fiscal_period           NUMBER(2),                  -- 1–12
    fiscal_quarter          VARCHAR(5),                 -- Q1, Q2, Q3, Q4
    period_start_date       DATE,
    period_end_date         DATE,
    is_period_open          BOOLEAN         DEFAULT TRUE,   -- TRUE while period is open
    is_year_end_period      BOOLEAN         DEFAULT FALSE,  -- TRUE for December (MY) / March (SG)
    working_day_flag        BOOLEAN         DEFAULT TRUE,   -- FALSE = public holiday
    public_holiday_name     VARCHAR(100)                    -- NULL if working day

    , CONSTRAINT pk_dim_fiscal_cal PRIMARY KEY (fiscal_period_key, date_key)
);


-- -----------------------------------------------------------------------------
-- 1.8  DIM_ROOT_CAUSE
-- Root cause codes for reconciliation exceptions.
-- 15 codes across SYSTEM, PROCESS, DATA, PAYMENT categories.
-- Powers the Page 5 Pareto chart and exception routing logic.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE dim_root_cause (
    root_cause_code         VARCHAR(10)     NOT NULL,   -- RC-001 through RC-015, DQ-*
    category                VARCHAR(20),                -- SYSTEM, PROCESS, DATA, PAYMENT
    subcategory             VARCHAR(50),                -- Batch Job, Tax Code, etc.
    description             VARCHAR(200),
    responsible_team        VARCHAR(30),                -- IT-SAP, FINANCE-MY, TREASURY
    typical_resolution_hrs  NUMBER(4),
    is_systemic             BOOLEAN         DEFAULT FALSE,  -- TRUE = structural fix needed
    occurrence_count        NUMBER(10)                      -- Running count (updated by pipeline)

    , CONSTRAINT pk_dim_root_cause PRIMARY KEY (root_cause_code)
);


-- =============================================================================
-- SECTION 2 — FACT TABLES (run after dimensions)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 2.1  POS_TRANSACTION_HEADER
-- Core POS event table. One row per sale, return, or void.
-- ~206,000 rows. Largest source of reconciliation breaks.
-- _dq_flag: CLEAN | RC-005 (duplicate) | NULL_STORE | WRONG_COUNTRY | FUTURE_DATE
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE pos_transaction_header (
    pos_txn_id              VARCHAR(50)     NOT NULL,   -- TXN-MY-20241001-05745DF5
    country_code            VARCHAR(3)      NOT NULL,   -- Partition key
    store_id                VARCHAR(20),
    terminal_id             VARCHAR(20),                -- TILL-03
    cashier_id              VARCHAR(20),                -- EMP-MY-20112
    business_date           DATE,
    txn_datetime_utc        TIMESTAMP_TZ,               -- Full timestamp with TZ
    txn_type                VARCHAR(10),                -- SALE, RETURN, VOID
    txn_status              VARCHAR(15),                -- COMPLETED, CANCELLED
    gross_amount            NUMBER(18,2),               -- Pre-discount amount
    discount_amount         NUMBER(18,2),               -- Discount applied
    net_amount              NUMBER(18,2),               -- gross - discount (pre-tax)
    tax_code                VARCHAR(10),                -- SR-6, TX9
    tax_amount_local        NUMBER(18,2),               -- Tax in local currency
    currency_code           VARCHAR(3),
    total_tendered          NUMBER(18,2),               -- Amount paid by customer
    promo_code              VARCHAR(50),                -- PROMO-MY-2024-023 (RC-003 source)
    receipt_number          VARCHAR(50),
    sap_company_code        VARCHAR(4),                 -- MY01, SG01
    sap_doc_number          VARCHAR(20),                -- SAP BELNR (NULL if RC-001)
    sap_post_status         VARCHAR(15),                -- POSTED, FAILED, NULL
    _dq_flag                VARCHAR(30),                -- CLEAN or issue code
    _dq_description         VARCHAR(200),

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_pos_txn PRIMARY KEY (pos_txn_id)
);


-- -----------------------------------------------------------------------------
-- 2.2  POS_TENDER
-- Payment tender lines. One or more rows per transaction (split tender).
-- ~202,000 rows. Drives the tender-type mismatch analysis on Page 2.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE pos_tender (
    pos_tender_id           VARCHAR(60)     NOT NULL,   -- TNDR-1-05745DF5-01
    pos_txn_id              VARCHAR(50)     NOT NULL,   -- FK to pos_transaction_header
    country_code            VARCHAR(3)      NOT NULL,
    tender_code             VARCHAR(20),                -- TNG, VISA, CASH, PAYNOW...
    tender_amount           NUMBER(18,2),
    currency_code           VARCHAR(3),
    ewallet_provider        VARCHAR(30),                -- TNG, GrabPay, Boost
    paynow_ref              VARCHAR(50),                -- SG PayNow reference
    duitnow_ref             VARCHAR(50),                -- MY DuitNow reference
    nets_ref                VARCHAR(50),                -- SG NETS reference
    card_last4              VARCHAR(4),                 -- Last 4 digits (masked)
    card_type               VARCHAR(10),                -- VISA, MASTERCARD, AMEX
    approval_code           VARCHAR(20),                -- Card auth code
    rrn                     VARCHAR(20),                -- Retrieval Reference Number
    settlement_batch        VARCHAR(60),                -- Links to bank_card_settlement
    business_date           DATE,
    _dq_flag                VARCHAR(30),

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_pos_tender PRIMARY KEY (pos_tender_id)
    , CONSTRAINT fk_pos_tender_txn FOREIGN KEY (pos_txn_id)
        REFERENCES pos_transaction_header(pos_txn_id)
);


-- -----------------------------------------------------------------------------
-- 2.3  POS_EOD_SUMMARY
-- Daily Z-report (end-of-day) totals per store.
-- ~2,730 rows (15 stores × ~182 days).
-- THIS is the primary driver of POS-SAP reconciliation — daily net sales per store.
-- _dq_flag: CLEAN | WRONG_TAX | NULL_STORE
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE pos_eod_summary (
    eod_summary_id          VARCHAR(50)     NOT NULL,   -- EOD-MY-001-T01-20241001
    country_code            VARCHAR(3)      NOT NULL,
    store_id                VARCHAR(20),
    terminal_id             VARCHAR(20),                -- TILL-ALL for store total
    business_date           DATE,
    fiscal_period_key       VARCHAR(15),                -- MYS-2024-10
    currency_code           VARCHAR(3),
    net_sales               NUMBER(18,2),               -- Total net sales (pre-tax)
    tax_collected           NUMBER(18,2),               -- SST/GST collected
    cash_variance           NUMBER(18,2),               -- Cash over/short
    paynow_total            NUMBER(18,2),               -- SG PayNow total
    duitnow_total           NUMBER(18,2),               -- MY DuitNow total
    nets_total              NUMBER(18,2),               -- SG NETS total
    sap_transfer_status     VARCHAR(15),                -- TRANSFERRED, PENDING, FAILED
    supervisor_id           VARCHAR(20),                -- Store supervisor who closed
    _dq_flag                VARCHAR(30),

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_pos_eod PRIMARY KEY (eod_summary_id)
);


-- -----------------------------------------------------------------------------
-- 2.4  SAP_FI_DOCUMENT  (mirrors SAP table BKPF)
-- SAP FI document headers. One row per accounting document.
-- ~200,000 rows. SAP document number is the matching key for reconciliation.
-- _dq_flag: CLEAN | RC-001 (batch fail) | RC-008 (wrong period)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE sap_fi_document (
    belnr                   VARCHAR(20)     NOT NULL,   -- SAP document number
    bukrs                   VARCHAR(4)      NOT NULL,   -- Company code: MY01, SG01
    gjahr                   NUMBER(4)       NOT NULL,   -- Fiscal year: 2024, 2025
    country_code            VARCHAR(3)      NOT NULL,   -- Partition key (derived from bukrs)
    blart                   VARCHAR(2),                 -- Document type: RV, KR, SA
    bldat                   DATE,                       -- Document date (business date)
    budat                   DATE,                       -- Posting date
    waers                   VARCHAR(3),                 -- Currency: MYR, SGD
    xblnr                   VARCHAR(50),                -- External reference (pos_txn_id)
    usnam                   VARCHAR(20),                -- Posted by: BATCH_POS_MY
    bstat                   VARCHAR(1),                 -- Status: ' '=active, 'R'=reversed
    stblg                   VARCHAR(20),                -- Reversal document (RC-008 cases)
    tax_country             VARCHAR(2),                 -- Tax country: MY, SG
    pos_txn_id              VARCHAR(50),                -- Linked POS transaction
    _dq_flag                VARCHAR(30),
    _manual_error           BOOLEAN         DEFAULT FALSE,  -- TRUE = injected error flag

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_sap_fi_doc PRIMARY KEY (belnr, bukrs, gjahr)
);


-- -----------------------------------------------------------------------------
-- 2.5  SAP_FI_LINEITEM  (mirrors SAP table BSEG)
-- SAP FI accounting line items. 3 rows per document on average.
-- ~600,000 rows. GL account (hkont) starting with '004' = revenue accounts.
-- _dq_flag: CLEAN | WRONG_TAX | RC-003 (promo gross posting)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE sap_fi_lineitem (
    belnr                   VARCHAR(20)     NOT NULL,   -- FK to sap_fi_document
    bukrs                   VARCHAR(4)      NOT NULL,
    gjahr                   NUMBER(4)       NOT NULL,
    buzei                   VARCHAR(3)      NOT NULL,   -- Line item number: 001, 002...
    hkont                   VARCHAR(20),                -- GL account number
    shkzg                   VARCHAR(1),                 -- S = debit, H = credit
    dmbtr                   NUMBER(18,2),               -- Amount in local currency
    hwae                    VARCHAR(3),                 -- Local currency: MYR, SGD
    augbl                   VARCHAR(20),                -- Clearing document (set when cleared)
    augdt                   DATE,                       -- Clearing date
    mwskz                   VARCHAR(5),                 -- Tax code: S6, TX
    kostl                   VARCHAR(20),                -- Cost centre
    prctr                   VARCHAR(20),                -- Profit centre
    sgtxt                   VARCHAR(200),               -- Line item text
    _dq_flag                VARCHAR(30),

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_sap_fi_line PRIMARY KEY (belnr, bukrs, gjahr, buzei)
    , CONSTRAINT fk_sap_fi_line_doc FOREIGN KEY (belnr, bukrs, gjahr)
        REFERENCES sap_fi_document(belnr, bukrs, gjahr)
);


-- -----------------------------------------------------------------------------
-- 2.6  BANK_CARD_SETTLEMENT
-- Payment processor settlement files. One row per settlement batch.
-- ~22,000 rows. Contains actual MDR charged vs contracted rate (RC-010 source).
-- _dq_flag: CLEAN | RC-010 (fee variance) | RC-014 (no bank credit)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE bank_card_settlement (
    settlement_id           VARCHAR(80)     NOT NULL,   -- SETTLE-BATCH-001-TNG-20241001-001
    country_code            VARCHAR(3)      NOT NULL,
    processor_name          VARCHAR(30),                -- TNG_DIGITAL_MY, VISA_SG
    merchant_id             VARCHAR(30),                -- MID-MY-001-001
    merchant_name           VARCHAR(50),
    txn_date                DATE,                       -- Transaction date
    settle_date             DATE,                       -- Settlement date (T+N)
    currency_code           VARCHAR(3),
    tender_code             VARCHAR(20),                -- TNG, VISA, NETS, PAYNOW
    txn_count               NUMBER(10),
    gross_amount            NUMBER(18,2),               -- Total gross sales in batch
    refund_amount           NUMBER(18,2),
    chargeback_amount       NUMBER(18,2),               -- RC-011 source
    interchange_fee         NUMBER(18,2),               -- Actual fee charged
    contracted_rate_pct     NUMBER(8,4),                -- Contracted MDR rate
    actual_rate_pct         NUMBER(8,4),                -- Actual MDR rate (RC-010: actual > contracted)
    net_settlement          NUMBER(18,2),               -- gross - fees - chargebacks
    settlement_batch_id     VARCHAR(80),                -- Internal batch reference
    bank_account            VARCHAR(50),                -- Destination bank account
    bank_code               VARCHAR(10),
    sap_clear_status        VARCHAR(15),                -- MATCHED, UNMATCHED, PARTIAL
    sap_clear_doc           VARCHAR(20),                -- SAP clearing document
    _dq_flag                VARCHAR(60),

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_bank_settlement PRIMARY KEY (settlement_id)
);


-- -----------------------------------------------------------------------------
-- 2.7  BANK_STATEMENT_LINE
-- MT940 bank statement credits. One row per bank transaction.
-- ~22,000 rows. Matched to bank_card_settlement to confirm cash receipt.
-- _dq_flag: CLEAN | RC-014 (no matching settlement)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE bank_statement_line (
    statement_line_id       VARCHAR(60)     NOT NULL,   -- BSL-MY-20241002-000001
    country_code            VARCHAR(3)      NOT NULL,
    bank_code               VARCHAR(10),
    bank_account            VARCHAR(50),
    currency_code           VARCHAR(3),
    value_date              DATE,                       -- Date credit appeared in bank
    amount                  NUMBER(18,2),
    dc_indicator            VARCHAR(1),                 -- C = credit, D = debit
    payment_rail            VARCHAR(30),                -- IBG / DuitNow, FAST, RTGS
    customer_reference      VARCHAR(100),               -- Processor batch reference
    fast_ref                VARCHAR(50),                -- SG FAST transaction reference
    narrative               VARCHAR(300),               -- Bank statement description
    sap_clear_status        VARCHAR(15),                -- MATCHED, UNMATCHED
    sap_clear_doc           VARCHAR(20),
    _dq_flag                VARCHAR(30),
    _settle_id              VARCHAR(80),                -- Linked settlement_id (derived)

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_bank_stmt PRIMARY KEY (statement_line_id)
);


-- =============================================================================
-- SECTION 3 — DERIVED TABLE (run last, after all facts and dimensions)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 3.1  RECON_MATCH_RESULT
-- Central reconciliation output. Powers all 5 Power BI pages.
-- ~25,000 rows. One row = one store × date × recon_type reconciliation event.
--
-- recon_type = 'POS_SAP'  → sales reconciliation (POS EOD vs SAP posting)
-- recon_type = 'PAY_BANK' → payment reconciliation (settlement vs bank credit)
--
-- match_type progression:
--   EXACT      → variance = 0 (auto-cleared)
--   TOLERANCE  → |variance| < threshold_t2 (auto-cleared, no human touch)
--   REVIEW     → threshold_t2 < |variance| < threshold_t3 (Finance reviews)
--   MANUAL     → |variance| > threshold_t3 but resolved manually
--   ESCALATE   → |variance| > threshold_t3, IT ticket raised
--   UNMATCHED  → no matching record found on either side
--
-- SOX controls: preparer_id (maker) ≠ reviewer_id (checker)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE recon_match_result (
    match_id                VARCHAR(80)     NOT NULL,   -- MATCH-POS-SAP-MY-20241001-001
    recon_type              VARCHAR(10)     NOT NULL,   -- POS_SAP | PAY_BANK
    country_code            VARCHAR(3)      NOT NULL,
    recon_date              DATE            NOT NULL,   -- Business date reconciled
    fiscal_period_key       VARCHAR(15),                -- MYS-2024-10
    store_id                VARCHAR(20),
    currency_code           VARCHAR(3),

    -- Amounts (three sides of the reconciliation)
    pos_amount              NUMBER(18,2),               -- POS EOD net sales
    sap_amount              NUMBER(18,2),               -- SAP posted amount
    bank_amount             NUMBER(18,2),               -- Bank credited amount (PAY_BANK)

    -- Variances
    variance_pos_sap        NUMBER(18,2),               -- pos_amount - sap_amount
    variance_sap_bank       NUMBER(18,2),               -- sap_amount - bank_amount

    -- Matching
    threshold_applied       NUMBER(18,2),               -- From dim_country at match time
    match_type              VARCHAR(15),                -- EXACT/TOLERANCE/REVIEW/MANUAL/ESCALATE/UNMATCHED
    match_status            VARCHAR(15),                -- MATCHED | EXCEPTION

    -- Exception detail
    root_cause_code         VARCHAR(10),                -- FK to dim_root_cause
    regulatory_ref          VARCHAR(50),                -- BNM/MAS reference if regulatory
    it_ticket_ref           VARCHAR(30),                -- INC-MY-2024-1001

    -- SOX maker-checker
    preparer_id             VARCHAR(20),                -- Finance analyst (maker)
    reviewer_id             VARCHAR(20),                -- Finance manager (checker)
    reviewed_at             TIMESTAMP_TZ,               -- When reviewer approved

    -- Data quality
    _dq_flag                VARCHAR(60),                -- CLEAN or injected issue code

    -- Audit trail
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    loaded_by               VARCHAR(100)    DEFAULT CURRENT_USER()

    , CONSTRAINT pk_recon_match PRIMARY KEY (match_id)
    , CONSTRAINT fk_recon_country FOREIGN KEY (country_code)
        REFERENCES dim_country(country_code)
    , CONSTRAINT fk_recon_store FOREIGN KEY (store_id)
        REFERENCES dim_store(store_id)
    , CONSTRAINT fk_recon_root_cause FOREIGN KEY (root_cause_code)
        REFERENCES dim_root_cause(root_cause_code)
);


-- =============================================================================
-- SECTION 4 — POST-CREATION: VERIFY TABLE COUNT
-- =============================================================================

-- Run this after all 16 tables are created to confirm structure
/*SELECT
    table_name,
    column_count,
    comment
FROM information_schema.tables
WHERE table_schema = 'RAW'
  AND table_type = 'BASE TABLE'
ORDER BY table_name;*/

-- Expected: 16 tables
-- dim_bank, dim_country, dim_currency, dim_fiscal_calendar,
-- dim_root_cause, dim_store, dim_tax_code, dim_tender_type (8 dims)
-- pos_eod_summary, pos_tender, pos_transaction_header (3 POS facts)
-- sap_fi_document, sap_fi_lineitem (2 SAP facts)
-- bank_card_settlement, bank_statement_line (2 bank facts)
-- recon_match_result (1 derived)


-- =============================================================================
-- SECTION 5 — COPY INTO: LOAD ALL 16 TABLES
-- =============================================================================
-- Prerequisites:
--   1. All 16 tables created (Section 1–3 above)
--   2. CSVs uploaded to stage via SnowSQL:
--      PUT file:///path/to/output/*.csv @noveomart_db.raw.csv_stage
--          AUTO_COMPRESS=TRUE;
--   3. Run the FILE FORMAT creation below FIRST

-- Load order matters: dimensions before facts, recon_match_result last.
-- =============================================================================

-- ── Create named file format (run once before any COPY INTO) ─────────────────

CREATE OR REPLACE FILE FORMAT noveomart_db.raw.csv_fmt
    TYPE                          = CSV
    SKIP_HEADER                   = 1
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    NULL_IF                       = ('')
    EMPTY_FIELD_AS_NULL           = TRUE
    DATE_FORMAT                   = 'YYYY-MM-DD'
    TIMESTAMP_FORMAT              = 'AUTO'
    TRIM_SPACE                    = TRUE;

-- Verify it was created
SHOW FILE FORMATS IN SCHEMA noveomart_db.raw;


-- ── STEP 1: Dimension tables ──────────────────────────────────────────────────

COPY INTO dim_country
FROM @csv_stage/dim_country.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_store
FROM @csv_stage/dim_store.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_tender_type
FROM @csv_stage/dim_tender_type.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_bank
FROM @csv_stage/dim_bank.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_tax_code
FROM @csv_stage/dim_tax_code.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_currency
FROM @csv_stage/dim_currency.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_fiscal_calendar
FROM @csv_stage/dim_fiscal_calendar.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO dim_root_cause
FROM @csv_stage/dim_root_cause.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';


-- ── STEP 2: Fact tables ───────────────────────────────────────────────────────

COPY INTO pos_transaction_header (
    pos_txn_id,
    country_code,
    store_id,
    terminal_id,
    cashier_id,
    business_date,
    txn_datetime_utc,
    txn_type,
    txn_status,
    gross_amount,
    discount_amount,
    net_amount,
    tax_code,
    tax_amount_local,
    currency_code,
    total_tendered,
    promo_code,
    receipt_number,
    sap_company_code,
    sap_doc_number,
    sap_post_status,
    _dq_flag,
    _dq_description
)
FROM @csv_stage/pos_transaction_header.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO pos_tender (
    pos_tender_id,
    pos_txn_id,
    country_code,
    tender_code,
    tender_amount,
    currency_code,
    ewallet_provider,
    paynow_ref,
    duitnow_ref,
    nets_ref,
    card_last4,
    card_type,
    approval_code,
    rrn,
    settlement_batch,
    business_date,
    _dq_flag
)
FROM @csv_stage/pos_tender.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO pos_eod_summary (
    eod_summary_id,
    country_code,
    store_id,
    terminal_id,
    business_date,
    fiscal_period_key,
    currency_code,
    net_sales,
    tax_collected,
    cash_variance,
    paynow_total,
    duitnow_total,
    nets_total,
    sap_transfer_status,
    supervisor_id,
    _dq_flag
)
FROM @csv_stage/pos_eod_summary.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO SAP_FI_DOCUMENT (
    belnr,
    bukrs,
    gjahr,
    country_code,
    blart,
    bldat,
    budat,
    waers,
    xblnr,
    usnam,
    bstat,
    stblg,
    tax_country,
    pos_txn_id,
    _dq_flag,
    _manual_error
)
FROM @csv_stage/sap_fi_document.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO sap_fi_lineitem (
    belnr,
    bukrs,
    gjahr,
    buzei,
    hkont,
    shkzg,
    dmbtr,
    hwae,
    augbl,
    augdt,
    mwskz,
    kostl,
    prctr,
    sgtxt,
    _dq_flag
)
FROM @csv_stage/sap_fi_lineitem.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO bank_card_settlement (
    settlement_id,
    country_code,
    processor_name,
    merchant_id,
    merchant_name,
    txn_date,
    settle_date,
    currency_code,
    tender_code,
    txn_count,
    gross_amount,
    refund_amount,
    chargeback_amount,
    interchange_fee,
    contracted_rate_pct,
    actual_rate_pct,
    net_settlement,
    settlement_batch_id,
    bank_account,
    bank_code,
    sap_clear_status,
    sap_clear_doc,
    _dq_flag
)
FROM @csv_stage/bank_card_settlement.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO bank_statement_line (
    statement_line_id,
    country_code,
    bank_code,
    bank_account,
    currency_code,
    value_date,
    amount,
    dc_indicator,
    payment_rail,
    customer_reference,
    fast_ref,
    narrative,
    sap_clear_status,
    sap_clear_doc,
    _dq_flag,
    _settle_id
)
FROM @csv_stage/bank_statement_line.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';


-- ── STEP 3: Derived table (load last) ─────────────────────────────────────────

COPY INTO recon_match_result (
    match_id,
    recon_type,
    country_code,
    recon_date,
    fiscal_period_key,
    store_id,
    currency_code,
    pos_amount,
    sap_amount,
    bank_amount,
    variance_pos_sap,
    variance_sap_bank,
    threshold_applied,
    match_type,
    match_status,
    root_cause_code,
    regulatory_ref,
    it_ticket_ref,
    preparer_id,
    reviewer_id,
    reviewed_at,
    _dq_flag
)
FROM @csv_stage/recon_match_result.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'noveomart_db.raw.csv_fmt')
ON_ERROR = 'ABORT_STATEMENT';

-- =============================================================================
-- SECTION 6 — VERIFY ROW COUNTS AFTER LOAD
-- =============================================================================

-- Run this after all COPY INTO commands to confirm expected row counts.
SELECT table_name, row_count
FROM noveomart_db.information_schema.tables
WHERE table_schema = 'RAW'
ORDER BY row_count DESC;

-- For more detail, including last altered timestamp:
SELECT
    table_name,
    row_count,
    last_altered
FROM information_schema.tables
WHERE table_schema  = 'RAW'
  AND table_type    = 'BASE TABLE'
ORDER BY
    CASE
        WHEN table_name LIKE 'dim_%'           THEN 1
        WHEN table_name LIKE 'pos_%'           THEN 2
        WHEN table_name LIKE 'sap_%'           THEN 3
        WHEN table_name LIKE 'bank_%'          THEN 4
        WHEN table_name = 'recon_match_result' THEN 5
    END,
    row_count DESC;

-- Expected results:
-- ─────────────────────────────────────────────────────
-- dim_currency           ~546    (daily FX rates)
-- dim_fiscal_calendar    ~364    (working days)
-- dim_tender_type         17
-- dim_root_cause          15
-- dim_store               15
-- dim_tax_code             6
-- dim_bank                 6
-- dim_country              2
-- ─────────────────────────────────────────────────────
-- pos_transaction_header  ~206,000
-- pos_tender              ~202,000
-- pos_eod_summary           ~2,730
-- ─────────────────────────────────────────────────────
-- sap_fi_lineitem         ~600,000
-- sap_fi_document         ~200,000
-- ─────────────────────────────────────────────────────
-- bank_card_settlement     ~22,000
-- bank_statement_line      ~22,000
-- ─────────────────────────────────────────────────────
-- recon_match_result       ~25,000
-- ─────────────────────────────────────────────────────
-- TOTAL                  ~1.28M
-- ─────────────────────────────────────────────────────


-- =============================================================================
-- SECTION 7 — SPOT CHECK QUERIES
-- Run these to confirm data quality after loading
-- =============================================================================

-- Check country distribution on largest table
SELECT country_code, COUNT(*) AS txn_count
FROM pos_transaction_header
GROUP BY country_code
ORDER BY txn_count DESC;
-- Expected: ~120K MYS, ~86K SGP

-- Confirm injected DQ breaks are present
SELECT _dq_flag, COUNT(*) AS cnt
FROM pos_transaction_header
WHERE _dq_flag <> 'CLEAN'
GROUP BY _dq_flag
ORDER BY cnt DESC;
-- Expected: RC-003 (~6,878), RC-005 (~1,861), WRONG_TAX (~192), NULL_STORE (~176), etc.

-- Confirm both recon types in match result
SELECT recon_type, match_status, COUNT(*) AS cnt
FROM recon_match_result
GROUP BY recon_type, match_status
ORDER BY recon_type, match_status;
-- Expected: POS_SAP MATCHED ~large, POS_SAP EXCEPTION ~smaller
--           PAY_BANK MATCHED ~large, PAY_BANK EXCEPTION ~smaller

-- Confirm SOX maker-checker separation
SELECT
    COUNT(*)                                            AS total_exceptions,
    COUNT(CASE WHEN preparer_id = reviewer_id
               THEN 1 END)                             AS same_person_violations,
    COUNT(CASE WHEN preparer_id <> reviewer_id
               OR reviewer_id IS NULL THEN 1 END)      AS correctly_segregated
FROM recon_match_result
WHERE match_status = 'EXCEPTION';
-- Expected: same_person_violations = 0 (SOX control enforced)

-- Variance distribution — confirm 4-tier routing is working
SELECT
    country_code,
    match_type,
    COUNT(*)                                           AS count,
    ROUND(AVG(ABS(variance_pos_sap)), 2)               AS avg_abs_variance,
    ROUND(MAX(ABS(variance_pos_sap)), 2)               AS max_abs_variance
FROM recon_match_result
WHERE recon_type = 'POS_SAP'
GROUP BY country_code, match_type
ORDER BY country_code, count DESC;

