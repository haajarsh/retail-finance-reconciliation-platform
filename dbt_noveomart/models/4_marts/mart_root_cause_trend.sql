-- =============================================================================
-- mart_root_cause_trend.sql
-- Layer    : Mart
-- Purpose  : Root cause trend analysis for both recon types
-- Grain    : One row = one root cause + one fiscal period + one country
-- Audience : Finance Manager / IT Manager (monthly trend review)
-- =============================================================================

WITH all_results AS (
    -- Combine both recon types for unified trend analysis
    SELECT
        match_id,
        country_code,
        recon_date,
        fiscal_period_key,
        store_id,
        match_status,
        root_cause_code,
        ABS(variance_pos_sap)   AS abs_variance,
        recon_type

    FROM {{ ref('int_pos_sap_matched') }}

    UNION ALL

    SELECT
        match_id,
        country_code,
        recon_date,
        NULL                    AS fiscal_period_key,
        store_id,
        match_status,
        root_cause_code,
        ABS(variance_sap_bank)  AS abs_variance,
        recon_type

    FROM {{ ref('int_pay_bank_matched') }}
),

root_cause AS (
    SELECT * FROM {{ ref('stg_dim_root_cause') }}
),

-- Derive fiscal period from date for PAY_BANK rows (which have no period key)
with_period AS (
    SELECT
        country_code,
        recon_type,
        store_id,

        -- Use fiscal_period_key if available, else derive from date
        COALESCE(
            fiscal_period_key,
            country_code || '-' || TO_VARCHAR(DATE_TRUNC('month', recon_date), 'YYYY-MM')
        )                       AS period_key,

        -- Extract year and month number for sorting
        YEAR(recon_date)        AS recon_year,
        MONTH(recon_date)       AS recon_month,

        -- Month label for Power BI axis (e.g. "Oct 24")
        TO_VARCHAR(recon_date, 'Mon YY') AS month_label,

        match_status,
        root_cause_code,
        abs_variance

    FROM all_results
),

-- Aggregate: count exceptions and sum variances by period + root cause
aggregated AS (
    SELECT
        country_code,
        recon_type,
        period_key,
        recon_year,
        recon_month,
        month_label,
        COALESCE(root_cause_code, 'UNCLASSIFIED') AS root_cause_code,

        -- Total records in this period (matched + exceptions)
        COUNT(*)                                    AS total_records,

        -- Exception count
        SUM(CASE WHEN match_status = 'EXCEPTION' THEN 1 ELSE 0 END)
                                                    AS exception_count,

        -- Match count
        SUM(CASE WHEN match_status = 'MATCHED' THEN 1 ELSE 0 END)
                                                    AS matched_count,

        -- Total variance amount
        SUM(abs_variance)                           AS total_variance_amount,

        -- Average variance per exception
        AVG(CASE WHEN match_status = 'EXCEPTION'
                 THEN abs_variance END)             AS avg_exception_variance

    FROM with_period
    GROUP BY
        country_code,
        recon_type,
        period_key,
        recon_year,
        recon_month,
        month_label,
        COALESCE(root_cause_code, 'UNCLASSIFIED')
),

-- Add root cause descriptions and calculate Pareto percentages
final AS (
    SELECT
        a.country_code,
        a.recon_type,
        a.period_key,
        a.recon_year,
        a.recon_month,
        a.month_label,
        a.root_cause_code,
        r.description               AS root_cause_description,
        r.category                  AS root_cause_category,
        r.responsible_team,
        r.is_systemic,
        a.total_records,
        a.exception_count,
        a.matched_count,
        a.total_variance_amount,
        a.avg_exception_variance,

        -- Match rate for this period + root cause combination
        ROUND(
            a.matched_count::FLOAT / NULLIF(a.total_records, 0) * 100,
            2
        )                           AS match_rate_pct,

        -- Exception rate
        ROUND(
            a.exception_count::FLOAT / NULLIF(a.total_records, 0) * 100,
            2
        )                           AS exception_rate_pct,

        -- Pareto: this root cause's share of ALL exceptions across all periods
        -- Used for the Pareto chart bar widths on Page 5
        ROUND(
            a.exception_count::FLOAT /
            NULLIF(SUM(a.exception_count) OVER (
                PARTITION BY a.country_code, a.recon_type
            ), 0) * 100,
            2
        )                           AS pareto_pct_of_all_exceptions,

        -- Cumulative Pareto percentage (for the 80/20 line)
        ROUND(
            SUM(a.exception_count) OVER (
                PARTITION BY a.country_code, a.recon_type
                ORDER BY a.exception_count DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )::FLOAT /
            NULLIF(SUM(a.exception_count) OVER (
                PARTITION BY a.country_code, a.recon_type
            ), 0) * 100,
            2
        )                           AS cumulative_pareto_pct

    FROM aggregated a
    LEFT JOIN root_cause r ON a.root_cause_code = r.root_cause_code
)

SELECT * FROM final
ORDER BY
    country_code,
    recon_year,
    recon_month,
    exception_count DESC
