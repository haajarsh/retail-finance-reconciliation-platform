-- =============================================================================
-- stg_pos_tender.sql
-- Layer   : Staging
-- Purpose : Cleans payment tender lines for each POS transaction
--           One transaction can have MULTIPLE tender rows (split payment). 
-- Source  : raw.pos_tender
-- =============================================================================
-- FIX APPLIED (v1.1):
--      Added JOIN to stg_pos_transaction_header to exclude tender rows
--      whose parent transaction was filtered out (NULL_STORE, CANCELLED).
--      This resolves the 167 orphaned relationship test failures.
--
--   ROOT CAUSE: 
--      stg_pos_transaction_header filters WHERE store_id IS NOT NULL
--      and WHERE txn_status = 'COMPLETED'. Tender rows for those filtered-out
--      transactions still existed in raw, causing the relationship test to fail.
--   Solution: 
--      inner join to the cleaned parent model — only keep tenders
--      whose parent transaction passed all staging filters.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'pos_tender') }}
),

-- Reference the already-cleaned parent model
-- INNER JOIN means: if the parent transaction was filtered out,
-- the tender row is also excluded. This keeps both tables in sync.
valid_transactions AS (
    SELECT pos_txn_id
    FROM {{ ref('stg_pos_transaction_header') }}
),
 

cleaned AS (
    SELECT
        t.pos_tender_id,
        t.pos_txn_id,
        UPPER(t.country_code)     AS country_code,
        UPPER(t.tender_code)      AS tender_code,
        ROUND(t.tender_amount, 2) AS tender_amount,
        UPPER(t.currency_code)    AS currency_code,
        t.ewallet_provider,
        t.paynow_ref,
        t.duitnow_ref,
        t.nets_ref,
        t.card_last4,
        t.card_type,
        t.approval_code,
        t.rrn,
        t.settlement_batch,
        t.business_date::DATE     AS business_date,
        t._dq_flag
 
    FROM source t
 
    -- INNER JOIN: only keep tender rows where parent transaction exists
    -- in the cleaned staging model (passed store_id and txn_status filters)
    INNER JOIN valid_transactions v
        ON t.pos_txn_id = v.pos_txn_id
 
    WHERE t.country_code IN ('MYS', 'SGP')
      AND t.tender_amount >= 0      -- Filter NEG_TENDER injected issues
 
)
 
SELECT * FROM cleaned