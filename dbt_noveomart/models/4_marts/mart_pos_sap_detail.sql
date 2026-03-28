-- =============================================================================
-- mart_pos_sap_detail.sql
-- Layer    : Mart
-- Purpose  : Detailed POS vs SAP reconciliation results
-- Grain    : One row = one store per day with its match result.
-- Audience : Finance Retail Analyst (daily working page)
-- =============================================================================
 
WITH pos_sap AS (
    SELECT * FROM {{ ref('int_pos_sap_matched') }}
),
 
store AS (
    SELECT * FROM {{ ref('stg_dim_store') }}
),
 
country AS (
    SELECT * FROM {{ ref('stg_dim_country') }}
),
 
root_cause AS (
    SELECT * FROM {{ ref('stg_dim_root_cause') }}
)
 
SELECT
    p.match_id,
    p.recon_type,
    p.country_code,
    p.recon_date,
    p.fiscal_period_key,
 
    -- Store details
    p.store_id,
    s.store_name,
    s.store_type,
    s.city,
    s.sap_profit_center,
 
    p.currency_code,
 
    -- The three reconciliation amounts
    p.pos_amount,
    p.sap_amount,
    p.variance_pos_sap,
 
    -- Variance as percentage of POS amount
    -- Useful for Page 2 bar chart fill calculation
    CASE
        WHEN p.pos_amount <> 0
        THEN ROUND(p.variance_pos_sap / p.pos_amount * 100, 4)
        ELSE 0
    END                             AS variance_pct,
 
    -- Absolute variance for threshold comparison
    ABS(p.variance_pos_sap)        AS abs_variance,
 
    -- Thresholds used at match time
    p.threshold_applied,
 
    -- Match classification
    p.match_type,
    p.match_status,
 
    -- Root cause details
    p.root_cause_code,
    r.description                   AS root_cause_description,
    r.responsible_team,
    r.typical_resolution_hrs,
    r.is_systemic,
 
    -- Country config
    c.regulatory_body,
    c.bank_transfer_rail,
 
    -- SAP detail
    p.sap_document_count,
 
    p._dq_flag
 
FROM pos_sap p
LEFT JOIN store s        ON p.store_id     = s.store_id
LEFT JOIN country c      ON p.country_code = c.country_code
LEFT JOIN root_cause r   ON p.root_cause_code = r.root_cause_code
