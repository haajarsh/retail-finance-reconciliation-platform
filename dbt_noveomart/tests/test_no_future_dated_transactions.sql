-- =============================================================================
-- tests/test_no_future_dated_transactions.sql
-- WHAT THIS TEST DOES:
--   Catches transactions with business dates in the future.
--   Future-dated transactions are a data quality issue (FUTURE_DATE flag)
--   that can inflate current period sales figures incorrectly.
--
-- EXPECTED RESULT: 0 rows (no future dates)
-- =============================================================================

SELECT
    pos_txn_id,
    business_date,
    country_code,
    store_id,
    _dq_flag
FROM {{ ref('stg_pos_transaction_header') }}
WHERE business_date > CURRENT_DATE()
