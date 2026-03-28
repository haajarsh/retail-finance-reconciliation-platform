-- =============================================================================
-- mart_exception_log.sql
-- Layer    : Mart
-- Purpose  : All open exceptions across both recon types in one table
-- Source   : int_pos_sap_matched + int_pay_bank_matched + stg_dim_store 
--                + stg_dim_root_cause + raw.recon_match_result
-- Grain    : One row = one exception needing human attention
-- Audience : Finance Analyst (daily exception working list),
--            Power BI Page 4 (drill-through from Pages 1/2/3, open items table)
-- =============================================================================
-- KEY FEATURES:
--   - Combines POS_SAP and PAY_BANK exceptions in one view
--   - Calculates exception age (how many days since it occurred)
--   - Flags SLA breaches (open more than 24 hours)
--   - Shows SOX maker-checker status from the raw recon table
-- =============================================================================
 
WITH all_exceptions AS (
 
    -- POS vs SAP exceptions only
    SELECT
        match_id,
        'POS_SAP'               AS recon_type,
        country_code,
        recon_date,
        store_id,
        currency_code,
        pos_amount,
        sap_amount,
        NULL::NUMBER            AS bank_amount,
        variance_pos_sap        AS primary_variance,
        match_type,
        match_status,
        root_cause_code,
        _dq_flag
 
    FROM {{ ref('int_pos_sap_matched') }}
    WHERE match_status = 'EXCEPTION'    -- Only unresolved exceptions
 
    UNION ALL
 
    -- Payment vs Bank exceptions only
    SELECT
        match_id,
        'PAY_BANK'              AS recon_type,
        country_code,
        recon_date,
        NULL::VARCHAR           AS store_id,
        currency_code,
        pos_amount,
        NULL::NUMBER            AS sap_amount,
        bank_amount,
        variance_sap_bank       AS primary_variance,
        match_type,
        match_status,
        root_cause_code,
        _dq_flag
 
    FROM {{ ref('int_pay_bank_matched') }}
    WHERE match_status = 'EXCEPTION'
 
),
 
store AS (
    SELECT * FROM {{ ref('stg_dim_store') }}
),
 
root_cause AS (
    SELECT * FROM {{ ref('stg_dim_root_cause') }}
),
 
-- Pull preparer/reviewer from the raw recon_match_result for SOX evidence
-- This is the only mart that reads from the raw source directly
-- because the SOX maker-checker data was not derived by our dbt models
raw_recon AS (
    SELECT
        match_id,
        preparer_id,
        reviewer_id,
        reviewed_at,
        it_ticket_ref
    FROM {{ source('raw', 'recon_match_result') }}
)
 
SELECT
    e.match_id,
    e.recon_type,
    e.country_code,
    e.recon_date,
 
    -- Exception age in days (from recon date to today)
    DATEDIFF(day, e.recon_date, CURRENT_DATE())     AS exception_age_days,
 
    -- SLA breach flag  - exceptions open more than 1 day breach the 24H SLA
    CASE
        WHEN DATEDIFF(day, e.recon_date, CURRENT_DATE()) > 1
        THEN 'SLA BREACHED'
        ELSE 'Within SLA'
    END                                             AS sla_status,
 
    -- Severity based on age
    CASE
        WHEN DATEDIFF(day, e.recon_date, CURRENT_DATE()) >= 5
            THEN 'CRITICAL'
        WHEN DATEDIFF(day, e.recon_date, CURRENT_DATE()) >= 3
            THEN 'HIGH'
        WHEN DATEDIFF(day, e.recon_date, CURRENT_DATE()) >= 1
            THEN 'MEDIUM'
        ELSE 'NEW'
    END                                             AS severity,
 
    -- Store details
    e.store_id,
    s.store_name,
    s.city,
 
    e.currency_code,
    e.pos_amount,
    e.sap_amount,
    e.bank_amount,
    e.primary_variance,
    ABS(e.primary_variance)                         AS abs_variance,
    e.match_type,
    e.match_status,
 
    -- Root cause details
    e.root_cause_code,
    r.description                                   AS root_cause_description,
    r.responsible_team,
    r.typical_resolution_hrs,
 
    -- SOX maker-checker
    rr.preparer_id,
    rr.reviewer_id,
    rr.reviewed_at,
    rr.it_ticket_ref,
 
    -- Has this been reviewed? (SOX checker sign-off)
    CASE
        WHEN rr.reviewer_id IS NOT NULL THEN 'REVIEWED'
        ELSE 'PENDING REVIEW'
    END                                             AS review_status,
 
    e._dq_flag
 
FROM all_exceptions e
LEFT JOIN store s       ON e.store_id      = s.store_id
LEFT JOIN root_cause r  ON e.root_cause_code = r.root_cause_code
LEFT JOIN raw_recon rr  ON e.match_id      = rr.match_id
 
ORDER BY
    e.recon_date DESC,
    ABS(e.primary_variance) DESC        -- Largest variances shown first