-- =============================================================================
-- int_sap_sales_by_store_date.sql
-- Layer   : Intermediate
-- Purpose : Aggregates SAP line items to get total posted revenue per store per business date.
--           SAP stores data at LINE ITEM level — one row per GL account posting.
--           A single store's daily sales creates many SAP documents and hundreds of line items.
--           Before we can compare SAP to POS, we must first collapse all those line items into one number per store per day.
-- Source  : stg_sap_fi_lineitem + stg_sap_fi_document
-- =============================================================================

WITH sap_lines AS (
    SELECT * FROM {{ ref('stg_sap_fi_lineitem') }}
),

sap_docs AS (
    SELECT * FROM {{ ref('stg_sap_fi_document') }}
),

-- Join line items to documents to get posting_date and country_code
-- (line items don't directly have the posting date — it's in the header)
sap_joined AS (
    SELECT
        sap_lines.belnr,
        sap_lines.bukrs,
        sap_docs.country_code,
        sap_docs.posting_date,             -- This is budat — the SAP posting date
        sap_docs.document_date,            -- This is bldat — the original business date
        sap_docs.pos_txn_id,
        sap_lines.hkont,                    -- GL account number
        sap_lines.signed_amount,            -- Pre-calculated in staging (+ = credit, - = debit)
        sap_lines.tax_code,
        sap_lines.profit_centre,            -- One profit centre per store in SAP
        sap_lines._dq_flag

    FROM sap_lines
    INNER JOIN sap_docs
        ON  sap_lines.belnr = sap_docs.belnr
        AND sap_lines.bukrs = sap_docs.bukrs
        AND sap_lines.gjahr = sap_docs.gjahr
),

-- Aggregate to store + date level
-- This collapses hundreds of line items into one number per store per day
sap_by_store_date AS (
    SELECT
        country_code,
        profit_centre                           AS sap_profit_centre,

        -- We reconcile on posting_date (budat), not document_date (bldat)
        -- RC-008 happens when these two dates are in different fiscal periods
        posting_date                            AS sap_posting_date,

        -- Filter to revenue GL accounts only
        -- hkont starting with '004' = revenue accounts (project convention)
        SUM(
            CASE
                WHEN hkont LIKE '004%' THEN signed_amount
                ELSE 0
            END
        )                                       AS sap_posted_revenue,

        -- Count of documents — useful for identifying RC-001 (batch failures)
        COUNT(DISTINCT belnr)                   AS sap_document_count,

        -- Flag if ANY line item has a DQ issue
        MAX(
            CASE WHEN _dq_flag <> 'CLEAN' THEN 1 ELSE 0 END
        )                                       AS has_dq_issue

    FROM sap_joined
    GROUP BY
        country_code,
        profit_centre,
        posting_date
)

SELECT * FROM sap_by_store_date
