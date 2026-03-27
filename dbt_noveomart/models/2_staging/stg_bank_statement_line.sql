-- =============================================================================
-- stg_bank_statement_line.sql
-- Layer   : Staging
-- Purpose : Cleans MT940 bank statement credit lines.
--           One row = one credit (or debit) on the bank statement.
-- Source  : raw.bank_statement_line
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'bank_statement_line') }}
),

cleaned AS (
    SELECT
        statement_line_id,
        UPPER(country_code)     AS country_code,
        bank_code,
        bank_account,
        UPPER(currency_code)    AS currency_code,
        value_date::DATE        AS value_date,
        ROUND(amount, 2)        AS amount,
        dc_indicator,           -- C = Credit (money in), D = Debit (money out)
        payment_rail,           -- IBG/DuitNow (MY), FAST (SG)
        customer_reference,     -- Processor batch reference for matching
        fast_ref,               -- Singapore FAST transaction reference
        narrative,              -- Bank statement description text
        sap_clear_status,
        sap_clear_doc,
        _dq_flag,
        _settle_id              -- Pre-linked settlement_id (from generator)

    FROM source

    WHERE country_code IN ('MYS', 'SGP')
      AND dc_indicator = 'C'    -- Credits only — we track inflows

)

SELECT * FROM cleaned
