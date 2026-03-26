"""
generate_data/generate_bank_payment.py
======================================
Generates bank and payment tables:
  bank_card_settlement.csv   (processor remittance files)
  bank_statement_line.csv    (MT940-style bank statement lines)

Injected breaks:
  - Settlement timing lag (BNPL T+2, Amex T+2, some T+3)
  - Interchange fee variance (actual ≠ contracted)
  - Missing bank credit (bank cut-off)
  - Partial settlement (chargeback mid-batch)
  - NETS settled separately from Visa/MC
  - PayNow real-time (T+0) vs card T+1
"""

import csv, os, random
from datetime import date, timedelta

random.seed(2026)
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

print("Loading tender data...")
tenders_raw = load_csv("pos_tender.csv")
stores_raw  = load_csv("dim_store.csv")
stores = {s["store_id"]: s for s in stores_raw}

# ── Group tender rows by (store, business_date, tender_code) ─
from collections import defaultdict
batches = defaultdict(lambda: {"amount": 0.0, "count": 0, "rrns": [], "currency": ""})

for t in tenders_raw:
    store_id = t.get("pos_txn_id","")[:14]  # not ideal — use store from tender
    # Rebuild store from tender pos_txn_id prefix: TXN-MY-/TXN-SG-
    country  = t["country_code"]
    b_date   = t["business_date"]
    tcode    = t["tender_code"]
    amt      = float(t["tender_amount"] or 0)
    currency = t["currency_code"]
    rrn      = t.get("rrn","")

    # Find store from settlement_batch
    batch_id = t.get("settlement_batch","")
    # batch format: BATCH-STR-VIS-YYYYMMDD-001 → store suffix is chars 6-8
    store_suffix = batch_id[6:9] if len(batch_id) > 9 else "001"

    # Resolve full store_id
    if country == "MYS":
        candidates = [s for s in stores if s.startswith("MY-STR")]
    else:
        candidates = [s for s in stores if s.startswith("SG-STR")]

    # Use settlement_batch store reference
    key = (batch_id, country, b_date, tcode, currency)
    batches[key]["amount"] += amt
    batches[key]["count"]  += 1
    batches[key]["currency"] = currency
    if rrn:
        batches[key]["rrns"].append(rrn)

# ── Interchange rates (contracted) ────────────────────────────
INTERCHANGE = {
    "VISA":          0.0150,
    "MASTERCARD":    0.0150,
    "AMEX":          0.0280,
    "UNIONPAY":      0.0080,
    "NETS":          0.0055,
    "MYDEBIT":       0.0015,
    "TNG":           0.0070,
    "GRABPAY_MY":    0.0070,
    "GRABPAY_SG":    0.0070,
    "BOOST":         0.0070,
    "DUITNOW_MY":    0.0000,
    "PAYNOW":        0.0000,
    "FAVE":          0.0080,
    "BNPL_ATOME_MY": 0.0350,
    "BNPL_ATOME_SG": 0.0350,
    "CASH":          0.0000,
    "VOUCHER":       0.0000,
}

# Settlement lag by tender
SETTLE_LAG = {
    "CASH":          0,
    "PAYNOW":        0,
    "DUITNOW_MY":    1,
    "VISA":          1,
    "MASTERCARD":    1,
    "NETS":          1,
    "MYDEBIT":       1,
    "TNG":           1,
    "GRABPAY_MY":    1,
    "GRABPAY_SG":    1,
    "BOOST":         1,
    "FAVE":          1,
    "UNIONPAY":      1,
    "AMEX":          2,
    "BNPL_ATOME_MY": 2,
    "BNPL_ATOME_SG": 2,
    "VOUCHER":       0,
}

# Processor per country+tender
def get_processor(country, tcode):
    if tcode == "NETS":          return "NETS_OPERATOR_SG"
    if tcode in ("PAYNOW",):     return "MAS_FAST_NETWORK"
    if tcode in ("DUITNOW_MY",): return "BNM_DUITNOW"
    if tcode in ("TNG",):        return "TNG_DIGITAL_MY"
    if tcode in ("BOOST",):      return "BOOST_MY"
    if tcode in ("GRABPAY_MY","GRABPAY_SG"): return "GRAB_FINANCIAL"
    if tcode in ("FAVE",):       return "FAVE_SG"
    if tcode in ("BNPL_ATOME_MY","BNPL_ATOME_SG"): return "ATOME_FINANCIAL"
    if country == "MYS":         return "MAYBANK_MERCHANT_SERVICES"
    return "DBS_MERCHANT_SERVICES"

settle_rows = []   # bank_card_settlement
bank_rows   = []   # bank_statement_line

# Bank account per country
BANK_ACCTS = {
    "MYS": {"account": "514-100001-001", "bank_code": "MAYB", "currency": "MYR"},
    "SGP": {"account": "003-100001-01",  "bank_code": "DBS",  "currency": "SGD"},
}

stmt_line_seq = {"MYS": 1, "SGP": 1}

for key, data in batches.items():
    batch_id, country, b_date, tcode, currency = key
    gross_amt = round(data["amount"], 2)
    count     = data["count"]

    if gross_amt <= 0 or tcode in ("CASH", "VOUCHER"):
        continue

    # Settlement lag
    lag = SETTLE_LAG.get(tcode, 1)
    # Inject occasional T+3 for BNPL
    if tcode in ("BNPL_ATOME_MY","BNPL_ATOME_SG") and random.random() < 0.15:
        lag = 3
    # Inject occasional missing bank credit (~0.5%)
    missing_credit = random.random() < 0.005
    if missing_credit:
        lag = 999  # will not appear in bank statement

    settle_date = (date.fromisoformat(b_date) + timedelta(days=lag)).isoformat()

    # Interchange
    contracted_rate = INTERCHANGE.get(tcode, 0.015)
    # Inject fee variance (~1% of batches)
    actual_rate = contracted_rate
    fee_variance_flag = "CLEAN"
    if random.random() < 0.010:
        actual_rate = round(contracted_rate * random.uniform(1.05, 1.25), 4)
        fee_variance_flag = "RC-010: Interchange fee variance — actual > contracted"

    gross_fee    = round(gross_amt * actual_rate, 2)
    # Chargeback mid-batch (~0.3%)
    chargeback   = 0.0
    chargeback_flag = ""
    if random.random() < 0.003 and gross_amt > 200:
        chargeback = round(random.uniform(50, min(500, gross_amt * 0.05)), 2)
        chargeback_flag = "RC-011: Chargeback deducted from batch"

    refund_total = 0.0  # simplified — returns handled separately in POS
    net_settle   = round(gross_amt - gross_fee - chargeback - refund_total, 2)

    processor    = get_processor(country, tcode)
    mid          = f"MID-{country[:2]}-{batch_id[6:9]}-001"

    dq_flag = fee_variance_flag or chargeback_flag or "CLEAN"
    if missing_credit:
        dq_flag = "RC-014: Bank credit not received — cut-off timing"

    settle_rows.append({
        "settlement_id":       f"SETTLE-{batch_id}",
        "country_code":        country,
        "processor_name":      processor,
        "merchant_id":         mid,
        "merchant_name":       f"NoveoMart {country[:2]}",
        "txn_date":            b_date,
        "settle_date":         settle_date,
        "currency_code":       currency,
        "tender_code":         tcode,
        "txn_count":           count,
        "gross_amount":        gross_amt,
        "refund_amount":       refund_total,
        "chargeback_amount":   chargeback,
        "interchange_fee":     gross_fee,
        "contracted_rate_pct": round(contracted_rate * 100, 4),
        "actual_rate_pct":     round(actual_rate * 100, 4),
        "net_settlement":      net_settle,
        "settlement_batch_id": batch_id,
        "bank_account":        BANK_ACCTS[country]["account"],
        "bank_code":           BANK_ACCTS[country]["bank_code"],
        "sap_clear_status":    "UNMATCHED",
        "sap_clear_doc":       "",
        "_dq_flag":            dq_flag,
    })

    # ── Bank statement line (appears on settle_date) ──────────
    if missing_credit:
        continue   # deliberate gap — no bank credit generated

    bank_acct = BANK_ACCTS[country]
    seq = stmt_line_seq[country]
    stmt_line_seq[country] += 1

    # Payment rail narrative
    if tcode == "PAYNOW":
        rail = "FAST / PayNow"
        fast_ref = f"FAST-{b_date.replace('-','')}-{random.randint(1000000,9999999)}"
        narrative = f"FAST SETTLEMENT NoveoMart SG MID {mid}"
    elif tcode == "DUITNOW_MY":
        rail = "IBG / DuitNow"
        fast_ref = ""
        narrative = f"DUITNOW SETTLEMENT NoveoMart MY {batch_id}"
    elif tcode == "NETS":
        rail = "NETS Operator"
        fast_ref = ""
        narrative = f"NETS DAILY SETTLEMENT MID {mid} {settle_date}"
    else:
        rail = "IBG / DuitNow" if country == "MYS" else "FAST / PayNow"
        fast_ref = ""
        narrative = f"{processor} SETTLEMENT {batch_id} {settle_date}"

    bank_rows.append({
        "statement_line_id":  f"BSL-{country[:2]}-{settle_date.replace('-','')}-{seq:06d}",
        "country_code":       country,
        "bank_code":          bank_acct["bank_code"],
        "bank_account":       bank_acct["account"],
        "currency_code":      currency,
        "value_date":         settle_date,
        "amount":             net_settle,
        "dc_indicator":       "C",
        "payment_rail":       rail,
        "customer_reference": batch_id,
        "fast_ref":           fast_ref,
        "narrative":          narrative,
        "sap_clear_status":   "UNMATCHED",
        "sap_clear_doc":      "",
        "_dq_flag":           "CLEAN",
        "_settle_id":         f"SETTLE-{batch_id}",
    })

write_csv("bank_card_settlement.csv", settle_rows, [
    "settlement_id","country_code","processor_name","merchant_id","merchant_name",
    "txn_date","settle_date","currency_code","tender_code","txn_count",
    "gross_amount","refund_amount","chargeback_amount","interchange_fee",
    "contracted_rate_pct","actual_rate_pct","net_settlement","settlement_batch_id",
    "bank_account","bank_code","sap_clear_status","sap_clear_doc","_dq_flag"])

write_csv("bank_statement_line.csv", bank_rows, [
    "statement_line_id","country_code","bank_code","bank_account","currency_code",
    "value_date","amount","dc_indicator","payment_rail","customer_reference",
    "fast_ref","narrative","sap_clear_status","sap_clear_doc","_dq_flag","_settle_id"])

print(f"\nBank/payment tables complete. {len(settle_rows):,} settlement batches, {len(bank_rows):,} bank lines.")