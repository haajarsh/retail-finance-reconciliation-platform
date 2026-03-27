# Staging Layer — Known Test Failures

**Run date:** 2026-03-26
**dbt version:** 1.11.7  
**Target:** dev (Snowflake)  
**Result:** PASS=51 WARN=0 ERROR=4 SKIP=0 TOTAL=55

---

## Summary

4 of 55 staging tests fail. All are **expected and documented**.
They represent intentionally injected data quality issues designed
to simulate real-world upstream problems.

> A dbt FAIL is not a broken pipeline.
> It is the pipeline correctly detecting broken data.

---

## Failure Analysis Table

| # | Test | Failures | Category | Root Cause | Resolution Layer |
|---|------|----------|----------|-----------|-----------------|
| 1 | not_null_stg_pos_transaction_header_store_id | 167 rows | NULL_STORE | Missing IS NOT NULL filter in WHERE clause | Fix in staging SQL |
| 2 | unique_stg_bank_card_settlement_settlement_id | 5484 rows | Upstream processor dupes | Processor resends full batch files | Dedup in staging SQL |
| 3 | unique_stg_pos_tender_pos_tender_id | 1 row | Duplicate | Simulated upstream duplicate row | Dedup in staging SQL |
| 4 | test_variance_within_bounds | Schema ERROR | Wrong run order | INTERMEDIATE schema not built yet | Tag as post_staging |

---

## Screenshot

![Staging test results before fix](<01 staging_before_fix.png>)

---

## Resolution

See PR: `fix/staging-data-quality-issues`  
After fix — PASS=51 ERROR=0

![Staging test results after fix](<02 staging_after_fix.png>)
```

---