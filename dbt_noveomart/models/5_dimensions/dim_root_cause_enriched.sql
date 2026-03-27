-- =============================================================================
-- models/5_dimensions/dim_root_cause_enriched.sql
-- PURPOSE  : Enriched root cause dimension with exception counts per code.
--            Powers the Pareto chart on Power BI Page 5.  
--            One row = one root cause code with its 6-month exception total.
-- =============================================================================

WITH root_cause AS (
    SELECT * FROM {{ ref('stg_dim_root_cause') }}
),

-- Get exception counts per root cause from the trend mart
exception_counts AS (
    SELECT
        root_cause_code,
        SUM(exception_count)        AS total_exceptions_6m,
        -- Pareto percentage across all root causes
        ROUND(
            SUM(exception_count)::FLOAT /
            NULLIF(SUM(SUM(exception_count)) OVER (), 0) * 100,
            2
        )                           AS pareto_pct

    FROM {{ ref('mart_root_cause_trend') }}
    WHERE root_cause_code <> 'UNCLASSIFIED'
    GROUP BY root_cause_code
)

SELECT
    r.root_cause_code,
    r.category,
    r.subcategory,
    r.description,
    r.responsible_team,
    r.typical_resolution_hrs,
    r.is_systemic,
    COALESCE(e.total_exceptions_6m, 0) AS total_exceptions_6m,
    COALESCE(e.pareto_pct, 0)          AS pareto_pct,

    -- Cumulative pareto for the 80/20 line on Page 5
    ROUND(
        SUM(COALESCE(e.total_exceptions_6m, 0)) OVER (
            ORDER BY COALESCE(e.total_exceptions_6m, 0) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )::FLOAT /
        NULLIF(SUM(COALESCE(e.total_exceptions_6m, 0)) OVER (), 0) * 100,
        2
    )                                   AS cumulative_pareto_pct,

    -- Rank by exception count (1 = most common = RC-003)
    RANK() OVER (
        ORDER BY COALESCE(e.total_exceptions_6m, 0) DESC
    )                                   AS pareto_rank

FROM root_cause r
LEFT JOIN exception_counts e ON r.root_cause_code = e.root_cause_code
ORDER BY pareto_rank
