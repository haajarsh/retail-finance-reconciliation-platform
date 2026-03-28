-- =============================================================================
-- stg_dim_tender_type.sql
-- Purpose: Clean and type-cast the raw dim_tender_type source
-- =============================================================================

with source as (
    select * from {{ source('raw', 'dim_tender_type') }}
),

cleaned as (
    select
        tender_code                         as tender_code,
        country_code                        as country_code,       -- 'MYS', 'SGP', or 'BOTH'
        tender_name                         as tender_name,
        tender_category                     as tender_category,    -- CASH, CARD, EWALLET, VOUCHER, BNPL
        sap_gl_account                      as sap_gl_account,
        cast(settlement_lag_days as int)    as settlement_lag_days,
        cast(is_card as boolean)            as is_card,
        cast(is_ewallet as boolean)         as is_ewallet,
        cast(interchange_rate_pct as float) as interchange_rate_pct,
        is_active                           as is_active,
        notes                               as notes
    from source
    where is_active = 'Y'    -- filter out any inactive tender types at staging
)

select * from cleaned