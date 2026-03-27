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
    all_exceptions.match_id,
    all_exceptions.recon_type,
    all_exceptions.country_code,
    all_exceptions.recon_date,

    -- Exception age in days (from recon date to today)
    DATEDIFF(day, all_exceptions.recon_date, CURRENT_DATE())     AS exception_age_days,

    -- SLA breach flag — exceptions open more than 1 day breach the 24H SLA
    CASE
        WHEN DATEDIFF(day, all_exceptions.recon_date, CURRENT_DATE()) > 1
        THEN 'SLA BREACHED'
        ELSE 'Within SLA'
    END                                             AS sla_status,

    -- Severity based on age
    CASE
        WHEN DATEDIFF(day, all_exceptions.recon_date, CURRENT_DATE()) >= 5
            THEN 'CRITICAL'
        WHEN DATEDIFF(day, all_exceptions.recon_date, CURRENT_DATE()) >= 3
            THEN 'HIGH'
        WHEN DATEDIFF(day, all_exceptions.recon_date, CURRENT_DATE()) >= 1
            THEN 'MEDIUM'
        ELSE 'NEW'
    END                                             AS severity,

    -- Store details
    all_exceptions.store_id,
    store.store_name,
    store.city,

    all_exceptions.currency_code,
    all_exceptions.pos_amount,
    all_exceptions.sap_amount,
    all_exceptions.bank_amount,
    all_exceptions.primary_variance,
    ABS(all_exceptions.primary_variance)                         AS abs_variance,
    all_exceptions.match_type,
    all_exceptions.match_status,

    -- Root cause details
    all_exceptions.root_cause_code,
    root_causedescription                                   AS root_cause_description,
    root_causeresponsible_team,
    root_causetypical_resolution_hrs,

    -- SOX maker-checker
    raw_recon.preparer_id,
    raw_recon.reviewer_id,
    raw_recon.reviewed_at,
    raw_recon.it_ticket_ref,

    -- Has this been reviewed? (SOX checker sign-off)
    CASE
        WHEN raw_recon.reviewer_id IS NOT NULL THEN 'REVIEWED'
        ELSE 'PENDING REVIEW'
    END                                             AS review_status,

    all_exceptions._dq_flag

FROM all_exceptions 
LEFT JOIN store         ON all_exceptions.store_id          = store.store_id
LEFT JOIN root_cause    ON all_exceptions.root_cause_code   = root_causeroot_cause_code
LEFT JOIN raw_recon     ON all_exceptions.match_id          = raw_recon.match_id

ORDER BY
    all_exceptions.recon_date DESC,
    ABS(all_exceptions.primary_variance) DESC        -- Largest variances shown first
