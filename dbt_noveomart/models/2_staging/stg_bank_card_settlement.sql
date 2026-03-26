-- =============================================================================
-- stg_bank_card_settlement.sql
-- Layer   : Staging
-- Purpose : Cleans payment processor settlement files.
--           One row = one settlement batch (by tender type, by day, by store).
-- Source  : raw.bank_card_settlement
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'bank_card_settlement') }}
),

cleaned AS (
    SELECT
        settlement_id,
        UPPER(country_code)                 AS country_code,
        processor_name,
        merchant_id,
        merchant_name,
        txn_date::DATE                      AS txn_date,
        settle_date::DATE                   AS settle_date,
        UPPER(currency_code)                AS currency_code,
        UPPER(tender_code)                  AS tender_code,
        txn_count,
        ROUND(gross_amount, 2)              AS gross_amount,
        ROUND(refund_amount, 2)             AS refund_amount,
        ROUND(chargeback_amount, 2)         AS chargeback_amount,
        ROUND(interchange_fee, 2)           AS interchange_fee,
        contracted_rate_pct,
        actual_rate_pct,
        ROUND(net_settlement, 2)            AS net_settlement,
        settlement_batch_id,
        bank_account,
        bank_code,
        sap_clear_status,
        sap_clear_doc,

        -- Calculated: fee overcharge amount
        -- Positive = processor charged MORE than contracted (RC-010)
        ROUND((actual_rate_pct - contracted_rate_pct)
              / 100 * gross_amount, 2)      AS fee_variance_amount,

        _dq_flag

    FROM source

    WHERE country_code IN ('MYS', 'SGP')

)

SELECT * FROM cleaned
