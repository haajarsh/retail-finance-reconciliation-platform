-- =============================================================================
-- stg_sap_fi_lineitem.sql
-- Layer   : Staging
-- Purpose : Cleans SAP FI line items (mirrors SAP table BSEG)
--           One row = one accounting line within a document.
--           Every document has multiple lines (debit the bank, credit the revenue account).
-- Source  : raw.pos_sap_fi_lineitem
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'sap_fi_lineitem') }}
),

cleaned AS (
    SELECT
        belnr,          -- FK to sap_fi_document
        bukrs,
        gjahr,
        buzei,          -- Line item number within the document
        hkont,          -- GL account number
        shkzg,          -- S = Debit, H = Credit

        -- Signed amount:
        -- Credit (H) on revenue account = positive revenue
        -- Debit (S) on revenue account = reversal/return
        CASE
            WHEN shkzg = 'H' THEN  ROUND(dmbtr, 2)
            WHEN shkzg = 'S' THEN -ROUND(dmbtr, 2)
            ELSE 0
        END             AS signed_amount,

        ROUND(dmbtr, 2) AS dmbtr,           -- Raw unsigned amount
        hwae            AS currency_code,
        augbl,          -- Clearing document (NULL if not yet cleared)
        augdt::DATE     AS clearing_date,
        mwskz           AS tax_code,
        kostl           AS cost_centre,
        prctr           AS profit_centre,
        sgtxt           AS line_text,
        _dq_flag

    FROM source

)

SELECT * FROM cleaned
