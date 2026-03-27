-- =============================================================================
-- stg_bank_card_settlement.sql
-- Layer   : Staging
-- Purpose : Cleans payment processor settlement files.
--           One row = one settlement batch (by tender type, by day, by store).
-- Source  : raw.bank_card_settlement
-- =============================================================================
-- FIX APPLIED (v1.1):
--      Added settlement_surrogate_key — a composite key combining
--      settlement_id + country_code to guarantee uniqueness.
--
--   ROOT CAUSE: The data generator created settlement_id using store number
--   only (e.g. store 007), without the country prefix. Both MY-STR-007
--   (Gurney) and SG-STR-007 (Changi) produced identical settlement_id values.
--   Since settlement_id alone is not globally unique across countries,
--   we create a surrogate key that IS unique.
--
-- KEY BUSINESS CONTEXT:
--   RC-010 = Interchange fee variance (actual_rate_pct > contracted_rate_pct)
--   RC-014 = Missing bank credit (settlement arrived, no bank line found)
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'bank_card_settlement') }}
),
 
cleaned AS (
    SELECT
        -- Composite surrogate key: settlement_id is only unique WITHIN a country
        -- Combining with country_code makes it globally unique
        settlement_id || '|' || country_code    AS settlement_surrogate_key,
 
        settlement_id,
        UPPER(country_code)                     AS country_code,
        processor_name,
        merchant_id,
        merchant_name,
        txn_date::DATE                          AS txn_date,
        settle_date::DATE                       AS settle_date,
        UPPER(currency_code)                    AS currency_code,
        UPPER(tender_code)                      AS tender_code,
        txn_count,
        ROUND(gross_amount, 2)                  AS gross_amount,
        ROUND(refund_amount, 2)                 AS refund_amount,
        ROUND(chargeback_amount, 2)             AS chargeback_amount,
        ROUND(interchange_fee, 2)               AS interchange_fee,
        contracted_rate_pct,
        actual_rate_pct,
        ROUND(net_settlement, 2)                AS net_settlement,
        settlement_batch_id,
        bank_account,
        bank_code,
        sap_clear_status,
        sap_clear_doc,
 
        -- Fee overcharge amount (RC-010 detection)
        ROUND(
            (actual_rate_pct - contracted_rate_pct)
            / 100 * gross_amount, 2
        )                                       AS fee_variance_amount,
 
        _dq_flag
 
    FROM source
 
    WHERE country_code IN ('MYS', 'SGP')
 
)
 
SELECT * FROM cleaned