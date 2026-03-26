-- =============================================================================
-- stg_pos_transaction_header.sql
-- Layer   : Staging
-- Purpose : Cleans individual POS transactions (sales, returns, voids).
--           One row = one POS transaction.
-- Source  : raw.pos_transaction_header
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'pos_transaction_header') }}
),

cleaned AS (
    SELECT
        pos_txn_id,
        UPPER(country_code)             AS country_code,
        store_id,
        terminal_id,
        cashier_id,
        business_date::DATE             AS business_date,

        -- Timestamp with timezone — kept as-is for audit trail
        txn_datetime_utc::TIMESTAMP_TZ  AS txn_datetime_utc,

        txn_type,       -- SALE, RETURN, VOID
        txn_status,     -- COMPLETED, CANCELLED

        -- Financial amounts
        ROUND(gross_amount, 2)          AS gross_amount,
        ROUND(discount_amount, 2)       AS discount_amount,
        ROUND(net_amount, 2)            AS net_amount,
        tax_code,
        ROUND(tax_amount_local, 2)      AS tax_amount_local,
        UPPER(currency_code)            AS currency_code,
        ROUND(total_tendered, 2)        AS total_tendered,

        -- promo_code: when populated on a SALE, it means
        -- the discount was applied at POS but may not exist in SAP
        promo_code,
        receipt_number,

        -- SAP linkage columns
        sap_company_code,
        sap_doc_number,     -- NULL when RC-001 (SAP batch job failed)
        sap_post_status,    -- POSTED, FAILED

        _dq_flag,
        _dq_description

    FROM source

    WHERE country_code IN ('MYS', 'SGP')
        AND txn_status = 'COMPLETED'    -- Exclude cancelled transactions
        AND store_id IS NOT NULL        -- ← ADD THIS: mirrors stg_pos_eod_summary pattern

)

SELECT * FROM cleaned