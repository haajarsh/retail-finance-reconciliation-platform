-- =============================================================================
-- models/5_dimensions/dim_store_enriched.sql
-- PURPOSE  : Enriched store dimension for Power BI relationships.
--            Adds calculated columns useful for reporting:
--              - Health score (calculated here from the mart summary)
--              - Exception count (rolling total)
--              - Store age in years
-- =============================================================================

WITH store AS (
    SELECT * FROM {{ ref('stg_dim_store') }}
),

-- Get exception counts per store from the recon summary
-- This gives Page 1 the "total exceptions" number per store
store_exception_counts AS (
    SELECT
        store_id,
        COUNT(*)                                    AS total_records,
        SUM(CASE WHEN match_status = 'EXCEPTION'
                 THEN 1 ELSE 0 END)                AS total_exceptions,
        SUM(CASE WHEN match_status = 'MATCHED'
                 THEN 1 ELSE 0 END)                AS total_matched,
        -- Sales match rate across all dates
        ROUND(
            SUM(CASE WHEN match_status = 'MATCHED' AND recon_type = 'POS_SAP'
                     THEN 1 ELSE 0 END)::FLOAT /
            NULLIF(SUM(CASE WHEN recon_type = 'POS_SAP'
                            THEN 1 ELSE 0 END), 0) * 100,
            2
        )                                           AS sales_match_rate_pct,
        -- Payment match rate across all dates
        ROUND(
            SUM(CASE WHEN match_status = 'MATCHED' AND recon_type = 'PAY_BANK'
                     THEN 1 ELSE 0 END)::FLOAT /
            NULLIF(SUM(CASE WHEN recon_type = 'PAY_BANK'
                            THEN 1 ELSE 0 END), 0) * 100,
            2
        )                                           AS payment_match_rate_pct

    FROM {{ ref('mart_recon_summary') }}
    GROUP BY store_id
)

SELECT
    s.store_id,
    s.country_code,
    s.store_name,
    s.mall_name,
    s.city,
    s.state,
    s.store_type,
    s.sap_profit_center,
    s.sap_cost_center,
    s.bank_account,
    s.bank_code,
    s.open_date,
    s.floor_area_sqft,
    s.num_terminals,

    -- Calculated fields for Power BI
    DATEDIFF(year, s.open_date, CURRENT_DATE())     AS store_age_years,

    -- Exception statistics
    COALESCE(e.total_records, 0)                    AS total_recon_records,
    COALESCE(e.total_exceptions, 0)                 AS total_exceptions,
    COALESCE(e.total_matched, 0)                    AS total_matched,
    COALESCE(e.sales_match_rate_pct, 0)             AS sales_match_rate_pct,
    COALESCE(e.payment_match_rate_pct, 0)           AS payment_match_rate_pct,

    -- Weighted health score
    -- 60% sales recon + 40% payment recon (same as DAX in Power BI)
    ROUND(
        COALESCE(e.sales_match_rate_pct, 0) * 0.6 +
        COALESCE(e.payment_match_rate_pct, 0) * 0.4,
        2
    )                                               AS health_score,

    -- Traffic light status (mirrors Power BI conditional formatting)
    CASE
        WHEN COALESCE(e.sales_match_rate_pct, 0) * 0.6 +
             COALESCE(e.payment_match_rate_pct, 0) * 0.4 >= 99.0
            THEN 'CLEAR'
        WHEN COALESCE(e.sales_match_rate_pct, 0) * 0.6 +
             COALESCE(e.payment_match_rate_pct, 0) * 0.4 >= 97.0
            THEN 'MONITOR'
        ELSE 'ESCALATE'
    END                                             AS health_status

FROM store s
LEFT JOIN store_exception_counts e ON s.store_id = e.store_id
