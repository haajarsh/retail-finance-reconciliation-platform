-- =============================================================================
-- models/marts/dim_tender_type_enriched.sql
-- Purpose: Tender type dimension for Power BI — payment method master
-- =============================================================================

{{ config(materialized='table') }}

with base as (
    select * from {{ ref('stg_dim_tender_type') }}
)

select
    tender_code,
    country_code,
    tender_name,
    tender_category,
    sap_gl_account,
    settlement_lag_days,
    is_card,
    is_ewallet,
    interchange_rate_pct,
    notes,

    -- Useful flag for Power BI measures
    case when settlement_lag_days = 0 then true else false end as is_instant_settlement

from base