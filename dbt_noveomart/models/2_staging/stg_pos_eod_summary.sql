-- =============================================================================
-- stg_pos_eod_summary.sql
-- Layer   : Staging
-- Purpose : Cleans and type-casts the POS daily Z-report summary.
--           This table drives the POS side of the POS-SAP reconciliation.
--           One row per store per business date.
--           No business logic here.
-- Source  : raw.pos_eod_summary
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'pos_eod_summary') }}

),

cleaned AS (

    SELECT
        eod_summary_id,
        UPPER(country_code)         AS country_code,
        store_id,
        terminal_id,
        business_date::DATE         AS business_date,
        fiscal_period_key,
        UPPER(currency_code)        AS currency_code,
        ROUND(net_sales, 2)         AS net_sales,
        ROUND(tax_collected, 2)     AS tax_collected,
        ROUND(cash_variance, 2)     AS cash_variance,

        -- Country-specific payment totals
        ROUND(paynow_total, 2)      AS paynow_total,    -- Singapore PayNow
        ROUND(duitnow_total, 2)     AS duitnow_total,   -- Malaysia DuitNow
        ROUND(nets_total, 2)        AS nets_total,      -- Singapore NETS

        sap_transfer_status,
        supervisor_id,

        -- _dq_flag carries the injected data quality issue code
        -- 'CLEAN' = no issue, 'NULL_STORE' = store_id was null, etc.
        _dq_flag

    FROM source

    -- ── FILTERS (data quality gates) ────────────────────────────────────────
    -- These WHERE conditions remove rows that should never reach the mart.
    -- Bad rows are counted in dbt tests — they are not silently dropped.

    WHERE business_date IS NOT NULL     -- Remove records with no date
      AND store_id      IS NOT NULL     -- Remove NULL_STORE injected issues
      AND country_code  IN ('MYS', 'SGP')  -- Remove WRONG_COUNTRY injected issues
)

SELECT * FROM cleaned