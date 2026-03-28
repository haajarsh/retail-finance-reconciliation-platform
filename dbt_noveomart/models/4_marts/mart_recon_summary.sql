-- =============================================================================
-- mart_recon_summary.sql
-- Layer    : Mart
-- Purpose  : Summary of all reconciliation results at store + date level
-- Grain    : One row = one store + one business date + one recon_type
-- Audience : Finance Controller (morning dashboard check)
-- =============================================================================
-- DESIGN PRINCIPLE:
--   No business logic in marts. This model only selects and shapes
--   from the intermediate models. All matching logic stays in
--   3_intermediate where it is independently testable.
-- =============================================================================
 
WITH pos_sap AS (
    -- POS vs SAP reconciliation results
    SELECT * FROM {{ ref('int_pos_sap_matched') }}
),
 
pay_bank AS (
    -- Payment vs Bank reconciliation results
    SELECT * FROM {{ ref('int_pay_bank_matched') }}
),
 
store AS (
    SELECT * FROM {{ ref('stg_dim_store') }}
),
 
-- Combine both reconciliation types into one summary table
-- UNION ALL keeps all rows from both models
combined AS (
 
    SELECT
        match_id,
        recon_type,                 -- 'POS_SAP'
        country_code,
        recon_date,
        fiscal_period_key,
        store_id,
        currency_code,
        pos_amount,
        sap_amount,
        0::NUMBER       AS bank_amount,
        variance_pos_sap,
        0::NUMBER       AS variance_sap_bank,
        match_type,
        match_status,
        root_cause_code,
        _dq_flag
 
    FROM pos_sap
 
    UNION ALL
 
    SELECT
        match_id,
        recon_type,                 -- 'PAY_BANK'
        country_code,
        recon_date,
        NULL            AS fiscal_period_key,
        store_id,
        currency_code,
        pos_amount,
        sap_amount,
        bank_amount,
        variance_pos_sap,
        variance_sap_bank,
        match_type,
        match_status,
        root_cause_code,
        _dq_flag
 
    FROM pay_bank
 
)
 
-- Final select: join store details for Power BI display
SELECT
    c.match_id,
    c.recon_type,
    c.country_code,
    c.recon_date,
    c.fiscal_period_key,
    c.store_id,
    s.store_name,
    s.store_type,
    s.city,
    c.currency_code,
    c.pos_amount,
    c.sap_amount,
    c.bank_amount,
    c.variance_pos_sap,
    c.variance_sap_bank,
    c.match_type,
    c.match_status,
    c.root_cause_code,
    c._dq_flag,
 
    -- Convenience flag for Power BI filtering
    CASE WHEN c.match_status = 'MATCHED' THEN 1 ELSE 0 END  AS is_matched,
    CASE WHEN c.match_status = 'EXCEPTION' THEN 1 ELSE 0 END AS is_exception
 
FROM combined c
LEFT JOIN store s
    ON c.store_id = s.store_id