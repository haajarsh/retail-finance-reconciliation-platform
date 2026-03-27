-- =============================================================================
-- int_sap_sales_by_store_date.sql
-- Layer   : Intermediate
-- Purpose : Aggregates SAP line items to get total posted revenue per store per business date.
--           SAP stores data at LINE ITEM level — one row per GL account posting.
--           A single store's daily sales creates many SAP documents and hundreds of line items.
--           Before we can compare SAP to POS, we must first collapse all those line items into one number per store per day.
-- Source  : stg_sap_fi_lineitem + stg_sap_fi_document
-- =============================================================================
--
-- FIX APPLIED (v1.1):
--   The data generator did not populate prctr (profit_centre) on any
--   SAP line item row — all 600,576 rows have empty prctr.
--   In real SAP this column is populated, but since the generator
--   skipped it, we derive store_id through the correct document chain:
--
--   sap_fi_lineitem
--     → sap_fi_document (via belnr + bukrs + gjahr)
--       → pos_transaction_header (via pos_txn_id)
--         → dim_store (via store_id)
--
--   This is actually more accurate than using prctr directly, because
--   it traces the posting back to the exact POS transaction that
--   triggered it — making the audit trail completely traceable.
--
-- READS FROM: staging models only
-- FEEDS INTO: int_pos_sap_matched.sql
-- =============================================================================
 
WITH sap_lines AS (
    SELECT * FROM {{ ref('stg_sap_fi_lineitem') }}
),
 
sap_docs AS (
    SELECT * FROM {{ ref('stg_sap_fi_document') }}
),
 
-- Bridge: POS transaction gives us store_id
-- This is the link between SAP and the physical store
pos_txn AS (
    SELECT
        pos_txn_id,
        store_id,
        country_code
    FROM {{ ref('stg_pos_transaction_header') }}
),
 
store AS (
    SELECT
        store_id,
        sap_profit_center
    FROM {{ ref('stg_dim_store') }}
),
 
-- Join line items → document → POS transaction → store
-- This chain gives us: which store does each SAP posting belong to?
sap_joined AS (
    SELECT
        l.belnr,
        l.bukrs,
        d.country_code,
        d.posting_date,
        d.document_date,
        d.pos_txn_id,
 
        -- Store identity derived from POS transaction chain
        -- (prctr is empty in generated data, so we use this path instead)
        p.store_id,
        s.sap_profit_center,
 
        l.hkont,
        l.signed_amount,
        l.tax_code,
        l._dq_flag
 
    FROM sap_lines l
    INNER JOIN sap_docs d
        ON  l.belnr = d.belnr
        AND l.bukrs = d.bukrs
        AND l.gjahr = d.gjahr
 
    -- Join to POS transaction to get store_id
    -- LEFT JOIN: some SAP docs may not have a pos_txn_id (e.g. manual postings)
    LEFT JOIN pos_txn p
        ON d.pos_txn_id = p.pos_txn_id
 
    -- Join to dim_store to get profit centre
    LEFT JOIN store s
        ON p.store_id = s.store_id
),
 
-- Aggregate to store + posting_date level
sap_by_store_date AS (
    SELECT
        country_code,
 
        -- Use store_id as the primary grouping key
        -- (more reliable than profit_centre in this dataset)
        store_id,
 
        -- Keep profit_centre for reference where available
        sap_profit_center                           AS sap_profit_centre,
 
        -- Reconcile on posting_date (budat), not document_date (bldat)
        posting_date                                AS sap_posting_date,
 
        -- Sum revenue GL accounts only (hkont starting with '004')
        SUM(
            CASE
                WHEN hkont LIKE '004%' THEN signed_amount
                ELSE 0
            END
        )                                           AS sap_posted_revenue,
 
        -- Document count — RC-001 shows as zero or null here
        COUNT(DISTINCT belnr)                       AS sap_document_count,
 
        -- DQ flag: 1 if any line item in this group has an issue
        MAX(
            CASE WHEN _dq_flag <> 'CLEAN' THEN 1 ELSE 0 END
        )                                           AS has_dq_issue
 
    FROM sap_joined
 
    -- Only aggregate where we have a valid store linkage
    -- Rows without store_id (no matching POS transaction) are excluded
    -- These would represent manual SAP postings outside the POS flow
    WHERE store_id IS NOT NULL
 
    GROUP BY
        country_code,
        store_id,
        sap_profit_center,
        posting_date
)
 
SELECT * FROM sap_by_store_date