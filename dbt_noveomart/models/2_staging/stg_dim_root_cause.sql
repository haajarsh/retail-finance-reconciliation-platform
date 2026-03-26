-- =============================================================================
-- stg_dim_root_cause.sql
-- Layer   : Staging
-- Purpose : Passes through the root cause dimension.
--           These codes appear on the recon_match_result table and
--           drive the Pareto chart on Power BI Page 5.
-- Source  : raw.dim_root_cause
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'dim_root_cause') }}
)

SELECT
    root_cause_code,
    category,           -- SYSTEM, PROCESS, DATA, PAYMENT
    subcategory,
    description,
    responsible_team,   -- IT-SAP, FINANCE-MY, TREASURY
    typical_resolution_hrs,
    is_systemic         -- Y = needs a permanent fix, not just daily cleanup

FROM source
