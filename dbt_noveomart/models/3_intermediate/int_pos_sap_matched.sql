-- =============================================================================
-- int_pos_sap_matched.sql
-- Layer   : Intermediate
-- Purpose : Compares POS daily totals against SAP posted revenue to identify variances.
--           Variance = POS amount - SAP amount.
--           One row = one store + one business date reconciliation result.
-- Source  : stg_pos_eod_summary + int_sap_sales_by_store_date + stg_dim_country + stg_dim_store
-- =============================================================================
-- FIX APPLIED (v1.1):
--   Updated JOIN to use store_id instead of sap_profit_centre.
--   int_sap_sales_by_store_date now groups by store_id (derived via
--   the pos_txn_id chain) rather than prctr (which was empty in the
--   generated dataset). The join is now:
--     p.store_id = s.store_id  (direct, clean, reliable)
--
-- THE MATCHING LOGIC:
--   EXACT      → variance = 0
--   TOLERANCE  → |variance| <= threshold_t2 (auto-cleared)
--   REVIEW     → threshold_t2 < |variance| <= threshold_t3
--   ESCALATE   → |variance| > threshold_t3
--   UNMATCHED  → no SAP record found (RC-001 batch failure)
--
-- WHY THRESHOLDS COME FROM dim_country (NOT hardcoded):
--   MYR 50 tolerance is correct for Malaysia.
--   SGD 20 tolerance is correct for Singapore.
--   Hardcoding either value would misclassify the other country.
-- =============================================================================
 
WITH pos AS (
    SELECT * FROM {{ ref('stg_pos_eod_summary') }}
),
 
sap AS (
    SELECT * FROM {{ ref('int_sap_sales_by_store_date') }}
),
 
cfg AS (
    SELECT * FROM {{ ref('stg_dim_country') }}
),
 
-- Join POS to SAP on store_id + business_date
-- LEFT JOIN on SAP: if no SAP record exists, sap_amount = NULL (RC-001)
joined AS (
    SELECT
        p.country_code,
        p.store_id,
        p.business_date                             AS recon_date,
        p.fiscal_period_key,
        p.currency_code,
 
        -- POS amount: net sales from Z-report
        p.net_sales                                 AS pos_amount,
 
        -- SAP amount: aggregated revenue from line items
        -- COALESCE to 0: if no SAP record, treat full POS amount as variance
        COALESCE(s.sap_posted_revenue, 0)           AS sap_amount,
 
        -- Variance: positive = POS > SAP, negative = SAP > POS
        p.net_sales - COALESCE(s.sap_posted_revenue, 0) AS variance_pos_sap,
 
        -- Config-driven thresholds from Dim_Country (never hardcoded)
        c.variance_threshold_t2                     AS threshold_tolerance,
        c.variance_threshold_t3                     AS threshold_review,
        c.variance_threshold_t4                     AS threshold_escalate,
 
        -- SAP metadata
        s.sap_document_count,
        s.sap_profit_centre,
        s.has_dq_issue                              AS sap_has_dq_issue,
 
        p.sap_transfer_status,
        p._dq_flag                                  AS pos_dq_flag
 
    FROM pos p
    LEFT JOIN sap s
        -- Join on store_id + posting_date (both now available in int_sap_*)
        ON  p.store_id      = s.store_id
        AND p.country_code  = s.country_code
        AND p.business_date = s.sap_posting_date
    JOIN cfg c
        ON  p.country_code  = c.country_code
),
 
-- Apply match classification
classified AS (
    SELECT
        *,
        ABS(variance_pos_sap)                       AS abs_variance,
 
        -- Match type: read from top, first match wins
        CASE
            WHEN ABS(variance_pos_sap) = 0
                THEN 'EXACT'
 
            WHEN ABS(variance_pos_sap) <= threshold_tolerance
                THEN 'TOLERANCE'                    -- RC-007: auto-cleared
 
            WHEN sap_document_count IS NULL OR sap_document_count = 0
                THEN 'UNMATCHED'                    -- RC-001: no SAP posting found
 
            WHEN ABS(variance_pos_sap) <= threshold_review
                THEN 'REVIEW'                       -- Finance manager reviews
 
            WHEN ABS(variance_pos_sap) > threshold_review
                THEN 'ESCALATE'                     -- IT ticket auto-raised
 
            ELSE 'UNMATCHED'
        END                                         AS match_type,
 
        -- Simplified status for Power BI traffic lights
        CASE
            WHEN ABS(variance_pos_sap) <= threshold_tolerance
                THEN 'MATCHED'
            ELSE 'EXCEPTION'
        END                                         AS match_status,
 
        -- Auto-detectable root causes
        CASE
            WHEN sap_document_count IS NULL OR sap_document_count = 0
                THEN 'RC-001'                       -- SAP batch failure
            WHEN ABS(variance_pos_sap) > 0
             AND ABS(variance_pos_sap) <= threshold_tolerance
                THEN 'RC-007'                       -- Rounding difference
            ELSE NULL                               -- Analyst classifies manually
        END                                         AS auto_root_cause_code
 
    FROM joined
)
 
SELECT
    -- Unique match identifier
    'MATCH-POS-SAP-' || country_code
        || '-' || TO_VARCHAR(recon_date, 'YYYYMMDD')
        || '-' || REPLACE(store_id, '-', '')        AS match_id,
 
    'POS_SAP'                                       AS recon_type,
    country_code,
    recon_date,
    fiscal_period_key,
    store_id,
    currency_code,
    pos_amount,
    sap_amount,
    variance_pos_sap,
    threshold_tolerance                             AS threshold_applied,
    match_type,
    match_status,
    auto_root_cause_code                            AS root_cause_code,
    sap_document_count,
    sap_profit_centre,
    pos_dq_flag                                     AS _dq_flag
 
FROM classified
