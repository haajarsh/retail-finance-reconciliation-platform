-- =============================================================================
-- stg_dim_country.sql
-- Layer   : Staging
-- Purpose : Passes through the dim_country configuration table with light cleaning.
--           This is the "brain" of the project — all country-specific thresholds
--            and settings are centralised here.
-- Source  : raw.dim_country
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'dim_country') }}
)

SELECT
    country_code,
    country_code_iso2,
    country_name,
    sap_company_code,
    base_currency,
    tax_system,
    tax_rate_standard,
    fiscal_year_end_month,
    timezone,
    settlement_lag_days,

    -- The three variance thresholds — these drive all exception routing
    variance_threshold_t2,   -- Below this = auto-clear (TOLERANCE)
    variance_threshold_t3,   -- Above this = escalate to Finance manager
    variance_threshold_t4,   -- Above this = escalate to IT + Controller

    regulatory_body,
    bank_transfer_rail,
    is_active

FROM source
WHERE is_active = 'Y'
