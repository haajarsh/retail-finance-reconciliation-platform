-- =============================================================================
-- int_pay_bank_matched.sql
-- Layer   : Intermediate
-- Purpose : Reconciles payment processor settlements against bank statement creditsettlements.
--           One row = one settlement batch matched (or not) to a bank credit.
-- Source  : stg_bank_card_settlement + stg_bank_statement_line + stg_dim_country
-- =============================================================================

WITH settlements AS (
    SELECT * FROM {{ ref('stg_bank_card_settlement') }}
),

bank_lines AS (
    SELECT * FROM {{ ref('stg_bank_statement_line') }}
),

cfg AS (
    SELECT * FROM {{ ref('stg_dim_country') }}
),

-- Match settlements to bank lines using the settlement reference
-- The settlement file puts its batch ID in the bank line's customer_reference
matched AS (
    SELECT
        settlements.settlement_id,
        settlements.country_code,
        settlements.processor_name,
        settlements.tender_code,
        settlements.txn_date,
        settlements.settle_date,
        settlements.currency_code,
        settlements.txn_count,
        settlements.gross_amount,
        settlements.interchange_fee,
        settlements.contracted_rate_pct,
        settlements.actual_rate_pct,
        settlements.net_settlement                        AS expected_bank_credit,
        settlements.fee_variance_amount,
        settlements.bank_account,
        settlements.bank_code,

        -- Bank line details (NULL if no match found = RC-014)
        bank_linesstatement_line_id,
        bank_linesvalue_date                            AS bank_credit_date,
        bank_linesamount                                AS actual_bank_credit,

        -- Variance: what we expected vs what the bank actually credited
        settlements.net_settlement - COALESCE(bank_linesamount, 0) AS variance_sap_bank,

        -- Days between transaction and bank receipt
        DATEDIFF(day, settlements.txn_date, COALESCE(bank_linesvalue_date, CURRENT_DATE()))
                                                AS settlement_lag_days_actual,
        cfg.settlement_lag_days                   AS settlement_lag_days_expected,

        settlements._dq_flag

    FROM settlements
    LEFT JOIN bank_lines
        -- Match on the processor batch reference embedded in bank narrative
        ON  settlements.country_code          = bank_linescountry_code
        AND settlements.bank_account          = bank_linesbank_account
        AND settlements.settlement_batch_id   = bank_linescustomer_reference
    JOIN cfg
        ON  settlements.country_code          = cfg.country_code
),

-- Classify each settlement match
classified AS (
    SELECT
        *,

        CASE
            -- Perfect match: expected = actual
            WHEN ABS(variance_sap_bank) = 0 AND statement_line_id IS NOT NULL
                THEN 'EXACT'

            -- Small rounding variance
            WHEN ABS(variance_sap_bank) <= 1.00 AND statement_line_id IS NOT NULL
                THEN 'TOLERANCE'

            -- Settlement found but fee variance exists (RC-010)
            WHEN statement_line_id IS NOT NULL AND ABS(fee_variance_amount) > 0
                THEN 'FEE_VARIANCE'

            -- No bank line found at all (RC-014)
            WHEN statement_line_id IS NULL
                THEN 'UNMATCHED'

            ELSE 'REVIEW'
        END                                     AS match_type,

        CASE
            WHEN statement_line_id IS NOT NULL AND ABS(variance_sap_bank) <= 1.00
                THEN 'MATCHED'
            ELSE 'EXCEPTION'
        END                                     AS match_status,

        -- Auto root cause detection
        CASE
            WHEN statement_line_id IS NULL
                THEN 'RC-014'   -- Missing bank credit
            WHEN ABS(fee_variance_amount) > 0
                THEN 'RC-010'   -- Interchange fee variance
            ELSE NULL
        END                                     AS root_cause_code

    FROM matched
)

SELECT
    'MATCH-PAY-BANK-' || country_code
        || '-' || TO_VARCHAR(txn_date, 'YYYYMMDD')
        || '-' || settlement_id                 AS match_id,

    'PAY_BANK'                                  AS recon_type,
    country_code,
    txn_date                                    AS recon_date,
    NULL::VARCHAR                               AS fiscal_period_key,
    NULL::VARCHAR                               AS store_id,
    currency_code,
    expected_bank_credit                        AS pos_amount,
    NULL::NUMBER                                AS sap_amount,
    COALESCE(actual_bank_credit, 0)             AS bank_amount,
    0::NUMBER                                   AS variance_pos_sap,
    variance_sap_bank,
    1.00                                        AS threshold_applied,
    match_type,
    match_status,
    root_cause_code,
    tender_code,
    processor_name,
    gross_amount,
    interchange_fee,
    fee_variance_amount,
    settlement_lag_days_actual,
    settlement_lag_days_expected,
    _dq_flag

FROM classified
