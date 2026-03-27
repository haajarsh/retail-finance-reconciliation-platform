-- =============================================================================
-- stg_sap_fi_document.sql
-- Layer   : Staging
-- Purpose : Cleans SAP FI document headers (mirrors SAP database table BKPF)
--           One row = one SAP accounting document
-- Source  : raw.pos_sap_fi_document
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'sap_fi_document') }}
),

cleaned AS (
    SELECT
        belnr,                              -- SAP document number (primary key)
        bukrs,                              -- Company code: MY01, SG01
        gjahr,                              -- Fiscal year
        UPPER(country_code) AS country_code,
        blart,                              -- Document type
        bldat::DATE         AS document_date,   -- Original business date
        budat::DATE         AS posting_date,    -- Date posted in SAP (used for recon)
        waers               AS currency_code,
        xblnr,                              -- External reference (= pos_txn_id)
        usnam,                              -- SAP user who posted (BATCH_POS_MY)
        bstat,                              -- Status: ' ' active, 'R' reversed
        stblg,                              -- Reversal document number (RC-008)
        tax_country,
        pos_txn_id,                         -- Linked POS transaction
        _dq_flag

    FROM source

    -- Only active documents — exclude reversed postings from recon
    -- (Reversed documents have their own reversal document to net to zero)
    WHERE country_code IN ('MYS', 'SGP')
      AND (bstat = ' ' OR bstat IS NULL)   -- Active documents only

)

SELECT * FROM cleaned