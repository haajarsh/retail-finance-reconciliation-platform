-- =============================================================================
-- tests/test_variance_within_bounds.sql
-- WHAT THIS TEST DOES:
--   Sanity check: ESCALATE exceptions should have variance ABOVE
--   the country's escalation threshold.
--   If an ESCALATE record has a small variance, the classification
--   logic is broken.
--
-- EXPECTED RESULT: 0 rows (no misclassified exceptions)
-- =============================================================================

SELECT
    p.match_id,
    p.country_code,
    p.match_type,
    p.variance_pos_sap,
    p.threshold_applied,
    c.variance_threshold_t3
FROM {{ ref('int_pos_sap_matched') }} p
JOIN {{ ref('stg_dim_country') }} c
    ON p.country_code = c.country_code
WHERE p.match_type = 'ESCALATE'
  AND ABS(p.variance_pos_sap) <= c.variance_threshold_t3
  -- An ESCALATE should NEVER have a variance below the escalation threshold
