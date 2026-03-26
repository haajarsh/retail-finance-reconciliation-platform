"""
data_generation/run_all.py
==========================
Master runner — executes all 5 generators in sequence
and prints a final dataset summary.

Usage:
    cd data_generation
    python run_all.py

Output:
    All CSVs written to data_generation/output/
"""

import subprocess, sys, os, csv, time
from datetime import datetime

START = time.time()
HERE  = os.path.dirname(os.path.abspath(__file__))
OUT   = os.path.join(HERE, "output")
os.makedirs(OUT, exist_ok=True)

SCRIPTS = [
    ("generate_dimensions.py",   "Dimension & reference tables"),
    ("generate_transactions.py", "POS transaction data (header, tender, EOD)"),
    ("generate_sap_fi.py",       "SAP FI documents & line items"),
    ("generate_bank_payment.py", "Bank settlement & statement lines"),
    ("generate_recon_match.py",  "Reconciliation match results"),
]

print("=" * 65)
print("  NoveoMart Portfolio — Synthetic Data Generator")
print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 65)

for script, desc in SCRIPTS:
    print(f"\n▶  {desc}")
    print(f"   ({script})")
    print("-" * 65)
    path = os.path.join(HERE, script)
    result = subprocess.run(
        [sys.executable, path],
        capture_output=False
    )
    if result.returncode != 0:
        print(f"\n❌ FAILED: {script}")
        sys.exit(1)

# ── Final summary ──────────────────────────────────────────────
elapsed = round(time.time() - START, 1)
print("\n" + "=" * 65)
print("  DATASET SUMMARY")
print("=" * 65)

CSV_FILES = [
    ("dim_country.csv",            "Dimension"),
    ("dim_currency.csv",           "Dimension"),
    ("dim_fiscal_calendar.csv",    "Dimension"),
    ("dim_tax_code.csv",           "Dimension"),
    ("dim_bank.csv",               "Dimension"),
    ("dim_store.csv",              "Dimension"),
    ("dim_tender_type.csv",        "Dimension"),
    ("dim_root_cause.csv",         "Dimension"),
    ("pos_transaction_header.csv", "Transactional"),
    ("pos_tender.csv",             "Transactional"),
    ("pos_eod_summary.csv",        "Transactional"),
    ("sap_fi_document.csv",        "Transactional"),
    ("sap_fi_lineitem.csv",        "Transactional"),
    ("bank_card_settlement.csv",   "Transactional"),
    ("bank_statement_line.csv",    "Transactional"),
    ("recon_match_result.csv",     "Derived"),
]

total_rows = 0
total_kb   = 0
print(f"\n  {'File':<38} {'Type':<15} {'Rows':>8}  {'KB':>7}")
print(f"  {'-'*38} {'-'*15} {'-'*8}  {'-'*7}")
for fname, ftype in CSV_FILES:
    fpath = os.path.join(OUT, fname)
    if not os.path.exists(fpath):
        print(f"  {'⚠ ' + fname:<38} {'MISSING':15} {'—':>8}  {'—':>7}")
        continue
    with open(fpath, newline="") as f:
        rows = sum(1 for _ in f) - 1   # subtract header
    kb = os.path.getsize(fpath) // 1024
    total_rows += rows
    total_kb   += kb
    print(f"  {fname:<38} {ftype:<15} {rows:>8,}  {kb:>6} KB")

print(f"\n  {'TOTAL':<38} {'':<15} {total_rows:>8,}  {total_kb:>6} KB")
print(f"\n  ✅ All files written to: {OUT}")
print(f"  ⏱  Total generation time: {elapsed}s")
print("=" * 65)

# ── Injected break summary ─────────────────────────────────────
print("\n  INJECTED BREAKS & DATA QUALITY ISSUE SUMMARY")
print(f"  {'File':<35} {'DQ Flag':<28} {'Count':>7}")
print(f"  {'-'*35} {'-'*28} {'-'*7}")

DQ_FILES = [
    "pos_transaction_header.csv",
    "pos_eod_summary.csv",
    "sap_fi_document.csv",
    "bank_card_settlement.csv",
    "recon_match_result.csv",
]
from collections import Counter
for fname in DQ_FILES:
    fpath = os.path.join(OUT, fname)
    if not os.path.exists(fpath):
        continue
    with open(fpath, newline="") as f:
        rdr = csv.DictReader(f)
        flags = Counter(r.get("_dq_flag","CLEAN") for r in rdr)
    for flag, cnt in sorted(flags.items(), key=lambda x: -x[1]):
        if flag and flag != "CLEAN":
            short_fname = fname.replace("_header","").replace(".csv","")
            print(f"  {short_fname:<35} {flag:<28} {cnt:>7,}")

print("\n  ℹ  Rows with _dq_flag = 'CLEAN' are intentionally clean baseline data.")
print("  ℹ  All injected issues are documented in DATA_DICTIONARY_README.md")
print("=" * 65 + "\n")