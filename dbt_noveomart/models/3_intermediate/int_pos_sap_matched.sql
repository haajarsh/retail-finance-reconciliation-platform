-- =============================================================================
-- int_pos_sap_matched.sql
-- Layer   : Intermediate
-- Purpose : Compares POS daily totals against SAP posted revenue to identify variances.
--           Variance = POS amount - SAP amount.
--           One row = one store + one business date reconciliation result.
-- Source  : stg_pos_eod_summary + int_sap_sales_by_store_date + stg_dim_country + stg_dim_store
-- =============================================================================


WITH pos AS (
    -- Daily POS totals per store
    SELECT * FROM {{ ref('stg_pos_eod_summary') }}
),

sap AS (
    -- SAP revenue aggregated to store+date level
    SELECT * FROM {{ ref('int_sap_sales_by_store_date') }}
),

-- Country configuration — thresholds and settings
cfg AS (
    SELECT * FROM {{ ref('stg_dim_country') }}
),

store AS (
    SELECT * FROM {{ ref('stg_dim_store') }}
),

-- Join POS to SAP and config
-- LEFT JOIN on SAP: if there's no SAP record, sap_amount will be NULL
-- This is how we detect RC-001 (SAP batch failure = no document = NULL)
joined AS (
    SELECT
        -- Identifiers
        pos.country_code,
        pos.store_id,
        pos.business_date                         AS recon_date,
        pos.fiscal_period_key,
        pos.currency_code,

        -- The three amounts being reconciled
        pos.net_sales                             AS pos_amount,
        COALESCE(sap.sap_posted_revenue, 0)       AS sap_amount,
        -- COALESCE: if SAP amount is NULL (no posting found), treat as 0
        -- This makes the variance = the full POS amount (worst case)

        -- The variance: positive = POS > SAP, negative = SAP > POS
        pos.net_sales - COALESCE(sap.sap_posted_revenue, 0) AS variance_pos_sap,

        -- Configuration thresholds from Dim_Country (NOT hardcoded)
       cfg.variance_threshold_t2                 AS threshold_tolerance,
       cfg.variance_threshold_t3                 AS threshold_review,
       cfg.variance_threshold_t4                 AS threshold_escalate,

        -- SAP metadata
       sap.sap_document_count,
       sap.has_dq_issue                          AS sap_has_dq_issue,

        -- POS metadata
        pos.sap_transfer_status,
        pos._dq_flag                              AS pos_dq_flag

    FROM pos
    LEFT JOIN sap
        ON  pos.country_code   = sap.country_code
        -- We match on store's SAP profit centre — this is how SAP identifies stores
        -- In production, you would have a mapping table.
        -- For this project, store_id maps to profit_centre by convention.
        AND pos.store_id       = sap.sap_profit_centre
        AND pos.business_date  = sap.sap_posting_date
    JOIN cfg
        ON  pos.country_code   = cfg.country_code
),

-- Apply matching classification logic
classified AS (
    SELECT
        *,

        -- Absolute variance (always positive — for threshold comparison)
        ABS(variance_pos_sap)                   AS abs_variance,

        -- ── MATCH TYPE CLASSIFICATION ──────────────────────────────────────
        -- This CASE statement is the core business rule.
        -- Read from top to bottom — first matching condition wins.
        CASE
            -- Perfect match: variance is exactly zero
            WHEN ABS(variance_pos_sap) = 0
                THEN 'EXACT'

            -- Rounding difference: very small variance below tolerance threshold
            -- These auto-clear with no human touch (RC-007)
            WHEN ABS(variance_pos_sap) <= threshold_tolerance
                THEN 'TOLERANCE'

            -- Medium variance: Finance manager reviews
            WHEN ABS(variance_pos_sap) <= threshold_review
                THEN 'REVIEW'

            -- No SAP record at all: batch job failure (RC-001)
            WHEN sap_document_count IS NULL OR sap_document_count = 0
                THEN 'UNMATCHED'

            -- Large variance: escalate to IT + Controller
            WHEN ABS(variance_pos_sap) > threshold_review
                THEN 'ESCALATE'

            -- Fallback (should never reach here)
            ELSE 'UNMATCHED'
        END                                     AS match_type,

        -- ── MATCH STATUS ───────────────────────────────────────────────────
        -- Simplified status for Power BI filter (P1 traffic lights)
        CASE
            WHEN ABS(variance_pos_sap) <= threshold_tolerance
                THEN 'MATCHED'      -- Auto-cleared (EXACT or TOLERANCE)
            ELSE 'EXCEPTION'        -- Needs human attention
        END                         AS match_status,

        -- ── ROOT CAUSE AUTO-DETECTION ──────────────────────────────────────
        -- We can automatically detect some root causes from the data.
        -- Finance analysts review and override as needed.
        CASE
            -- RC-001: No SAP documents found = batch failure
            WHEN sap_document_count IS NULL OR sap_document_count = 0
                THEN 'RC-001'

            -- RC-007: Very small rounding variance
            WHEN ABS(variance_pos_sap) > 0
              AND ABS(variance_pos_sap) <= threshold_tolerance
                THEN 'RC-007'

            -- For larger variances, we cannot auto-detect without more context.
            -- RC-003 (promo), RC-008 (wrong period) need the txn-level detail.
            -- Leave as NULL for Finance analyst to classify.
            ELSE NULL
        END                         AS auto_root_cause_code

    FROM joined
)

SELECT
    -- Generate a unique match_id for this reconciliation record
    -- Format: MATCH-POS-SAP-{country}-{date}-{store}
    'MATCH-POS-SAP-' || country_code
        || '-' || TO_VARCHAR(recon_date, 'YYYYMMDD')
        || '-' || REPLACE(store_id, '-', '')    AS match_id,

    'POS_SAP'                                   AS recon_type,
    country_code,
    recon_date,
    fiscal_period_key,
    store_id,
    currency_code,
    pos_amount,
    sap_amount,
    variance_pos_sap,
    threshold_tolerance                         AS threshold_applied,
    match_type,
    match_status,
    auto_root_cause_code                        AS root_cause_code,
    sap_document_count,
    pos_dq_flag                                 AS _dq_flag

FROM classified
