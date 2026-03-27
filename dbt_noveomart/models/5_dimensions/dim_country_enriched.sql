-- =============================================================================
-- models/5_dimensions/dim_country_enriched.sql
-- PURPOSE  : Enriched country dimension for Power BI slicers and filters.
--            Adds summary statistics per country for the Page 1 country filter.
-- =============================================================================

WITH country AS (
    SELECT * FROM {{ ref('stg_dim_country') }}
),

country_stats AS (
    SELECT
        country_code,
        COUNT(*)                                    AS total_recon_records,
        SUM(CASE WHEN match_status = 'EXCEPTION'
                 THEN 1 ELSE 0 END)                AS total_exceptions,
        ROUND(
            SUM(CASE WHEN match_status = 'MATCHED'
                     THEN 1 ELSE 0 END)::FLOAT /
            NULLIF(COUNT(*), 0) * 100, 2
        )                                           AS overall_match_rate_pct

    FROM {{ ref('mart_recon_summary') }}
    GROUP BY country_code
)

SELECT
    c.country_code,
    c.country_code_iso2,
    c.country_name,
    c.sap_company_code,
    c.base_currency,
    c.tax_system,
    c.tax_rate_standard,
    c.fiscal_year_end_month,
    c.settlement_lag_days,
    c.variance_threshold_t2,
    c.variance_threshold_t3,
    c.variance_threshold_t4,
    c.regulatory_body,
    c.bank_transfer_rail,

    -- Summary stats for Power BI country slicer display
    COALESCE(s.total_recon_records, 0)              AS total_recon_records,
    COALESCE(s.total_exceptions, 0)                 AS total_exceptions,
    COALESCE(s.overall_match_rate_pct, 0)           AS overall_match_rate_pct,

    -- Descriptive label for Power BI slicer
    -- Example: "🇲🇾 Malaysia (MYR)" or "🇸🇬 Singapore (SGD)"
    CASE
        WHEN c.country_code = 'MYS' THEN '🇲🇾 Malaysia (MYR)'
        WHEN c.country_code = 'SGP' THEN '🇸🇬 Singapore (SGD)'
        ELSE c.country_name
    END                                             AS country_display_label

FROM country c
LEFT JOIN country_stats s ON c.country_code = s.country_code
