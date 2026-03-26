-- =============================================================================
-- stg_pos_tender.sql
-- Layer   : Staging
-- Purpose : Cleans payment tender lines for each POS transaction
--           One transaction can have MULTIPLE tender rows (split payment). 
-- Source  : raw.pos_tender
-- =============================================================================-- 

WITH source AS (
    SELECT * FROM {{ source('raw', 'pos_tender') }}
),

cleaned AS (
    SELECT
        pos_tender_id,
        pos_txn_id,                             -- FK to pos_transaction_header
        UPPER(country_code)     AS country_code,
        UPPER(tender_code)      AS tender_code, -- VISA, CASH, TNG, PAYNOW, NETS...
        ROUND(tender_amount, 2) AS tender_amount,
        UPPER(currency_code)    AS currency_code,
        ewallet_provider,                       -- TNG, GrabPay, Boost (MY)
        paynow_ref,                             -- Singapore PayNow reference
        duitnow_ref,                            -- Malaysia DuitNow reference
        nets_ref,                               -- Singapore NETS reference
        card_last4,                             -- Last 4 digits only (PCI masked)
        card_type,                              -- VISA, MASTERCARD, AMEX
        approval_code,
        rrn,                                    -- Retrieval Reference Number
        settlement_batch,                       -- Links to bank_card_settlement
        business_date::DATE     AS business_date,
        _dq_flag

    FROM source

    WHERE country_code IN ('MYS', 'SGP')
      AND tender_amount >= 0      -- Filter out NEG_TENDER injected issues

)

SELECT * FROM cleaned