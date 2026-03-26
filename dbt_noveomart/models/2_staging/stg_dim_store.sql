-- =============================================================================
-- stg_dim_store.sql
-- Layer   : Staging
-- Purpose : Passes through the store dimension.
--           This table contains all relevant information about each store.
-- Source  : raw.dim_store
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'dim_store') }}
)

SELECT
    store_id,
    country_code,
    store_name,
    mall_name,
    city,
    state,
    store_type,                 -- FLAGSHIP, STANDARD
    sap_profit_center,
    sap_cost_center,
    bank_account,
    bank_code,
    tax_registration,
    store_manager,
    open_date::DATE  AS open_date,
    is_active,
    floor_area_sqft,
    num_terminals

FROM source
WHERE is_active = 'Y'
