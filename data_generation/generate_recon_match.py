"""
generate_data/generate_recon_match.py
=====================================
Generates the reconciliation output table:
  recon_match_result.csv

Logic:
  POS EOD Summary ↔ SAP FI Document (sales recon)
  Bank Card Settlement ↔ Bank Statement Line (payment recon)

Match types:
  EXACT      — amounts match within 0.01
  TOLERANCE  — within country threshold
  AGGREGATED — batch-level match
  MANUAL     — exception cleared by finance manually
  UNMATCHED  — open exception

This is the table that directly powers Power BI Pages 1-5.
"""

import csv, os, random, uuid
from datetime import date, timedelta
from collections import defaultdict

random.seed(2027)
OUT = os.path.join(os.path.dirname(__file__), "output")

def load_csv(filename):
    path = os.path.join(OUT, filename)
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv(name, rows, fieldnames):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {name:50s}  ({len(rows):>8,} rows)")

print("Loading source tables for reconciliation...")
eod_rows     = load_csv("pos_eod_summary.csv")
sap_docs     = load_csv("sap_fi_document.csv")
settlements  = load_csv("bank_card_settlement.csv")
bank_stmts   = load_csv("bank_statement_line.csv")
stores_list  = load_csv("dim_store.csv")
dim_country  = load_csv("dim_country.csv")

stores = {s["store_id"]: s for s in stores_list}
country_cfg = {c["country_code"]: c for c in dim_country}

# Thresholds (T3 = escalate)
THRESH = {
    "MYS": {"t2": 50.0,  "t3": 200.0,  "t4": 1000.0},
    "SGP": {"t2": 20.0,  "t3": 100.0,  "t4": 500.0},
}

# ── Finance team pools ────────────────────────────────────────
PREPARERS = {
    "MYS": ["EMP-MY-20145","EMP-MY-20146","EMP-MY-20147"],
    "SGP": ["EMP-SG-10089","EMP-SG-10090","EMP-SG-10091"],
}
REVIEWERS = {
    "MYS": ["EMP-MY-10023","EMP-MY-10024"],
    "SGP": ["EMP-SG-10054","EMP-SG-10055"],
}

# ── Regulatory references ─────────────────────────────────────
REG_REFS = {
    "MYS": ["BNM/RH/PD 028-2","BNM/RH/GN 003","RMCD-SST-GUIDE-2023"],
    "SGP": ["MAS-PSA-2019-01","MAS-NOTICE-PSN01","IRAS-GST-GUIDE-2024"],
}

recon_rows = []

# ═══════════════════════════════════════════════════════════════
# PART 1 — POS vs SAP SALES RECONCILIATION
# Match: POS EOD Summary ↔ SAP FI Documents (aggregated by store+date)
# ═══════════════════════════════════════════════════════════════
print("  Running POS vs SAP sales reconciliation...")

# Build SAP posted amounts per (company, posting_date) aggregated
sap_agg = defaultdict(float)
sap_dq  = defaultdict(list)
for doc in sap_docs:
    key = (doc["bukrs"], doc["budat"])
    # Sum the net amount from line items — approximate from xblnr
    # Use a simple proxy: BKPF-level net is reconstructed from POS
    # In real world you'd join BSEG; here we carry it
    # We'll key by (company, bldat) for matching
    key2 = (doc["bukrs"], doc["bldat"])
    dq = doc["_dq_flag"]
    if dq not in ("SAP_BATCH_FAIL","NULL_STORE","WRONG_COUNTRY","FUTURE_DATE"):
        sap_dq[key2].append(dq)

# Build SAP amounts keyed by (company, bldat) — sum of what SAP received
sap_posted = defaultdict(float)
# We re-read SAP lineitem credits to sales GL
sap_lines = load_csv("sap_fi_lineitem.csv")
sap_doc_map = {d["belnr"]+"_"+d["bukrs"]: d for d in sap_docs}
for line in sap_lines:
    bk = sap_doc_map.get(line["belnr"]+"_"+line["bukrs"])
    if not bk:
        continue
    # Sales revenue lines only (hkont starts with 004)
    if line["hkont"].startswith("004"):
        sign = 1 if line["shkzg"] == "H" else -1
        sap_posted[(bk["bukrs"], bk["bldat"])] += sign * float(line["dmbtr"])

# Map company to country
company_country = {"MY01":"MYS","SG01":"SGP"}

# Process each EOD row
problem_stores = {"MY-STR-004","SG-STR-003"}

ticket_seq = {"MYS": 1000, "SGP": 2000}

for eod in eod_rows:
    country   = eod["country_code"]
    store_id  = eod["store_id"]
    b_date    = eod["business_date"]
    currency  = eod["currency_code"]
    pos_net   = float(eod["net_sales"])
    fp_key    = eod["fiscal_period_key"]
    dq_flag   = eod["_dq_flag"]

    company = "MY01" if country == "MYS" else "SG01"
    sap_amount = sap_posted.get((company, b_date), 0.0)
    sap_amount = round(sap_amount, 2)

    variance = round(pos_net - sap_amount, 2)
    abs_var  = abs(variance)
    thresh   = THRESH[country]

    # Match type determination
    if dq_flag == "SAP_BATCH_FAIL":
        match_type   = "UNMATCHED"
        match_status = "EXCEPTION"
        root_cause   = "RC-001"
    elif abs_var <= 0.01:
        match_type   = "EXACT"
        match_status = "MATCHED"
        root_cause   = ""
    elif abs_var <= thresh["t2"]:
        match_type   = "TOLERANCE"
        match_status = "MATCHED"
        root_cause   = "RC-007" if abs_var < 1.0 else ""
    elif abs_var <= thresh["t3"]:
        match_type   = "TOLERANCE"
        match_status = "EXCEPTION"
        root_cause   = "RC-003" if "PROMO" in str(dq_flag) else "RC-007"
    else:
        match_type   = "UNMATCHED"
        match_status = "EXCEPTION"
        # Determine root cause from EOD dq
        rc_map = {
            "SAP_BATCH_FAIL":  "RC-001",
            "PROMO_UNMAPPED":  "RC-003",
            "ROUNDING_DIFF":   "RC-007",
            "WRONG_PERIOD":    "RC-008",
            "DUPLICATE_TXN":   "RC-005",
        }
        root_cause = rc_map.get(dq_flag, "RC-003")

    # Problem stores: force some additional exceptions
    if store_id in problem_stores and random.random() < 0.12:
        if match_status == "MATCHED":
            match_status = "EXCEPTION"
            match_type   = "TOLERANCE"
            variance     = round(random.uniform(-180, -30), 2)
            abs_var      = abs(variance)
            root_cause   = random.choice(["RC-003","RC-001","RC-007"])

    # Recent exceptions (last 30 days of data) — some still open
    d_obj = date.fromisoformat(b_date)
    is_recent = d_obj >= date(2025, 3, 1)

    it_ticket = ""
    if match_status == "EXCEPTION":
        if abs_var >= thresh["t3"]:
            ticket_seq[country] += 1
            it_ticket = f"INC-{country[:2]}-2024-{ticket_seq[country]:04d}" if d_obj.year == 2024 \
                        else f"INC-{country[:2]}-2025-{ticket_seq[country]:04d}"

    # Resolution: older exceptions are resolved; recent ones may still be open
    if match_status == "EXCEPTION" and not is_recent:
        # 85% of older exceptions resolved
        if random.random() < 0.85:
            match_status = "MATCHED"
            match_type   = "MANUAL"

    if match_status == "EXCEPTION" and is_recent:
        # 40% still open
        if random.random() < 0.40:
            match_status = "EXCEPTION"  # stays open

    preparer = random.choice(PREPARERS[country])
    reviewer = random.choice([r for r in REVIEWERS[country] if r != preparer] or REVIEWERS[country])
    reg_ref  = random.choice(REG_REFS[country]) if match_status == "EXCEPTION" else ""

    recon_rows.append({
        "match_id":           f"MATCH-POS-SAP-{country[:2]}-{b_date.replace('-','')}-{store_id[-3:]}",
        "recon_type":         "POS_SAP",
        "country_code":       country,
        "recon_date":         b_date,
        "fiscal_period_key":  fp_key,
        "store_id":           store_id,
        "currency_code":      currency,
        "pos_amount":         pos_net,
        "sap_amount":         sap_amount,
        "bank_amount":        0.0,
        "variance_pos_sap":   variance,
        "variance_sap_bank":  0.0,
        "threshold_applied":  thresh["t3"],
        "match_type":         match_type,
        "match_status":       match_status,
        "root_cause_code":    root_cause,
        "regulatory_ref":     reg_ref,
        "it_ticket_ref":      it_ticket,
        "preparer_id":        preparer,
        "reviewer_id":        reviewer if match_status == "MATCHED" else "",
        "reviewed_at":        f"{b_date}T17:30:00+08:00" if match_status == "MATCHED" else "",
        "_dq_flag":           dq_flag,
    })

# ═══════════════════════════════════════════════════════════════
# PART 2 — PAYMENT vs BANK RECONCILIATION
# Match: Bank Card Settlement ↔ Bank Statement Line
# ═══════════════════════════════════════════════════════════════
print("  Running Payment vs Bank reconciliation...")

# Index bank statement lines by customer_reference (= settlement_batch_id)
bank_by_batch = {b["customer_reference"]: b for b in bank_stmts}

for settle in settlements:
    country    = settle["country_code"]
    batch_id   = settle["settlement_batch_id"]
    settle_dt  = settle["settle_date"]
    currency   = settle["currency_code"]
    net_settle = float(settle["net_settlement"])
    dq_flag    = settle["_dq_flag"]
    tcode      = settle["tender_code"]
    fp_key     = f"{country}-{settle_dt[:4]}-{settle_dt[5:7]}"
    # Store approximation from batch ID
    store_sfx  = batch_id[6:9] if len(batch_id) > 9 else "001"
    store_id   = f"{'MY' if country=='MYS' else 'SG'}-STR-{store_sfx}"

    # Find matching bank statement line
    bank_line  = bank_by_batch.get(batch_id)
    bank_amount = float(bank_line["amount"]) if bank_line else 0.0

    variance = round(net_settle - bank_amount, 2)
    abs_var  = abs(variance)
    thresh   = THRESH[country]

    if "RC-014" in dq_flag:   # missing bank credit
        match_type   = "UNMATCHED"
        match_status = "EXCEPTION"
        root_cause   = "RC-014"
    elif "RC-011" in dq_flag: # chargeback
        match_type   = "UNMATCHED"
        match_status = "EXCEPTION"
        root_cause   = "RC-011"
    elif "RC-010" in dq_flag: # fee variance
        match_type   = "TOLERANCE"
        match_status = "EXCEPTION"
        root_cause   = "RC-010"
    elif abs_var <= 0.01:
        match_type   = "EXACT"
        match_status = "MATCHED"
        root_cause   = ""
    elif abs_var <= thresh["t2"]:
        match_type   = "TOLERANCE"
        match_status = "MATCHED"
        root_cause   = ""
    else:
        match_type   = "UNMATCHED"
        match_status = "EXCEPTION"
        root_cause   = "RC-009"

    # Settlement timing lag exceptions (BNPL / AMEX T+2/T+3)
    if tcode in ("BNPL_ATOME_MY","BNPL_ATOME_SG","AMEX") and match_status == "MATCHED":
        if random.random() < 0.05:  # occasional lag exception
            match_status = "EXCEPTION"
            match_type   = "UNMATCHED"
            root_cause   = "RC-009"
            variance     = round(-net_settle, 2)  # bank not yet received
            bank_amount  = 0.0

    d_obj = date.fromisoformat(settle_dt)
    is_recent = d_obj >= date(2025, 3, 1)

    it_ticket = ""
    if match_status == "EXCEPTION":
        ticket_seq[country] += 1
        it_ticket = f"INC-{country[:2]}-{d_obj.year}-{ticket_seq[country]:04d}"

    if match_status == "EXCEPTION" and not is_recent:
        if random.random() < 0.80:
            match_status = "MATCHED"
            match_type   = "MANUAL"

    preparer = random.choice(PREPARERS[country])
    reviewer = random.choice([r for r in REVIEWERS[country] if r != preparer] or REVIEWERS[country])
    reg_ref  = random.choice(REG_REFS[country]) if match_status == "EXCEPTION" else ""

    recon_rows.append({
        "match_id":           f"MATCH-PAY-BANK-{country[:2]}-{settle_dt.replace('-','')}-{batch_id[-6:]}",
        "recon_type":         "PAY_BANK",
        "country_code":       country,
        "recon_date":         settle_dt,
        "fiscal_period_key":  fp_key,
        "store_id":           store_id,
        "currency_code":      currency,
        "pos_amount":         0.0,
        "sap_amount":         net_settle,
        "bank_amount":        bank_amount,
        "variance_pos_sap":   0.0,
        "variance_sap_bank":  variance,
        "threshold_applied":  thresh["t3"],
        "match_type":         match_type,
        "match_status":       match_status,
        "root_cause_code":    root_cause,
        "regulatory_ref":     reg_ref,
        "it_ticket_ref":      it_ticket,
        "preparer_id":        preparer,
        "reviewer_id":        reviewer if match_status == "MATCHED" else "",
        "reviewed_at":        f"{settle_dt}T17:30:00+08:00" if match_status == "MATCHED" else "",
        "_dq_flag":           dq_flag,
    })

write_csv("recon_match_result.csv", recon_rows, [
    "match_id","recon_type","country_code","recon_date","fiscal_period_key",
    "store_id","currency_code","pos_amount","sap_amount","bank_amount",
    "variance_pos_sap","variance_sap_bank","threshold_applied",
    "match_type","match_status","root_cause_code","regulatory_ref",
    "it_ticket_ref","preparer_id","reviewer_id","reviewed_at","_dq_flag"])

# ── Summary stats ──────────────────────────────────────────────
pos_sap = [r for r in recon_rows if r["recon_type"] == "POS_SAP"]
pay_bank = [r for r in recon_rows if r["recon_type"] == "PAY_BANK"]
ps_exc = [r for r in pos_sap if r["match_status"] == "EXCEPTION"]
pb_exc = [r for r in pay_bank if r["match_status"] == "EXCEPTION"]

print(f"\n  POS vs SAP : {len(pos_sap):,} records | {len(ps_exc):,} exceptions ({100*len(ps_exc)/max(1,len(pos_sap)):.1f}%)")
print(f"  Pay vs Bank: {len(pay_bank):,} records | {len(pb_exc):,} exceptions ({100*len(pb_exc)/max(1,len(pay_bank)):.1f}%)")
print(f"\nRecon match result table complete. {len(recon_rows):,} total rows.")