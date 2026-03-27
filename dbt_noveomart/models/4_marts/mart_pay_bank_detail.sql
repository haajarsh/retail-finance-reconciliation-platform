-- =============================================================================
-- mart_pay_bank_detail.sql
-- Layer    : Mart
-- Purpose  : Detailed payment vs bank reconciliation results
-- Grain    : One row = one settlement batch with its match result.
-- Audience : Treasury Analyst (cash completeness verification)
-- =============================================================================

WITH pay_bank AS (
    SELECT * FROM {{ ref('int_pay_bank_matched') }}
),

country AS (
    SELECT * FROM {{ ref('stg_dim_country') }}
),

root_cause AS (
    SELECT * FROM {{ ref('stg_dim_root_cause') }}
)

SELECT
    p.match_id,
    p.recon_type,
    p.country_code,
    p.recon_date,
    p.currency_code,

    -- Payment processor details
    p.tender_code,
    p.processor_name,

    -- Settlement amounts
    p.gross_amount,
    p.interchange_fee,
    p.fee_variance_amount,

    -- Contracted vs actual rate
    -- These columns come directly from the intermediate model

    -- Expected vs actual bank credit
    p.pos_amount                    AS expected_bank_credit,
    p.bank_amount                   AS actual_bank_credit,
    p.variance_sap_bank,

    -- Settlement timing
    p.settlement_lag_days_actual,
    p.settlement_lag_days_expected,

    -- Is this overdue? (actual lag > expected lag)
    CASE
        WHEN p.settlement_lag_days_actual > p.settlement_lag_days_expected
        THEN 'OVERDUE'
        ELSE 'ON TIME'
    END                             AS settlement_timing_status,

    -- How many days overdue?
    GREATEST(
        p.settlement_lag_days_actual - p.settlement_lag_days_expected,
        0
    )                               AS days_overdue,

    -- Match result
    p.match_type,
    p.match_status,
    p.root_cause_code,
    r.description                   AS root_cause_description,
    r.responsible_team,

    -- Aging bucket — for Page 3 aging analysis
    -- Groups overdue settlements by how late they are
    CASE
        WHEN p.settlement_lag_days_actual <= p.settlement_lag_days_expected
            THEN 'Current'
        WHEN p.settlement_lag_days_actual <= p.settlement_lag_days_expected + 1
            THEN 'T+1 Overdue'
        WHEN p.settlement_lag_days_actual <= p.settlement_lag_days_expected + 2
            THEN 'T+2 Overdue'
        ELSE '>T+2 Overdue'
    END                             AS aging_bucket,

    c.regulatory_body,
    p._dq_flag

FROM pay_bank p
LEFT JOIN country c     ON p.country_code    = c.country_code
LEFT JOIN root_cause r  ON p.root_cause_code = r.root_cause_code
