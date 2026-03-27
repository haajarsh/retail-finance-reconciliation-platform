-- =============================================================================
-- tests/test_sox_maker_checker.sql
-- WHAT THIS TEST DOES:
--   Verifies SOX maker-checker control:
--   The person who prepares a reconciliation (preparer_id)
--   must NEVER be the same person who reviews it (reviewer_id).
--   This is a fundamental internal controls requirement.
--
-- HOW dbt SINGULAR TESTS WORK:
--   dbt runs this query. If it returns ANY rows, the test FAILS.
--   If it returns zero rows, the test PASSES.
--   So we write the query to return the VIOLATIONS we want to catch.
--
-- EXPECTED RESULT: 0 rows (no SOX violations)
-- =============================================================================

SELECT
    match_id,
    preparer_id,
    reviewer_id,
    recon_date,
    country_code
FROM {{ source('raw', 'recon_match_result') }}
WHERE preparer_id IS NOT NULL
  AND reviewer_id IS NOT NULL
  AND preparer_id = reviewer_id   -- Same person = SOX violation
