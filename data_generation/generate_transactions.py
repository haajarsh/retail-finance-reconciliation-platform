"""
generate_data/generate_transactions.py
======================================
Generates POS transactional tables:
  pos_transaction_header.csv
  pos_tender.csv
  pos_eod_summary.csv

Key design decisions:
  - ~180 business days × 15 stores × avg 60 txns/day ≈ 150,000+ transaction lines
  - Realistic sales patterns: weekends higher, Dec peak, Jan dip post-holiday
  - Country-accurate tender mixes (SG heavy PayNow/NETS, MY heavy TnG/Cash)
  - Injected breaks and data quality issues clearly flagged in _dq_flag column
"""

import csv, os, random, uuid
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

random.seed(2024)

OUT = os.path.join(os.path.dirname(__file__), "output")

# ─── Load dimension tables we need ───────────────────────────
def load_dim(filename, key_col):
    path = os.path.join(OUT, filename)
    rows = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows[r[key_col]] = r
    return rows

STORES   = list(load_dim("dim_store.csv",      "store_id").values())
TENDERS  = load_dim("dim_tender_type.csv", "tender_code")
HOLIDAYS = {}
with open(os.path.join(OUT,"dim_fiscal_calendar.csv"), newline="") as f:
    for r in csv.DictReader(f):
        if r["working_day_flag"] == "N":
            HOLIDAYS[(r["country_code"], r["date_key"])] = r["public_holiday_name"]

MY_STORES = [s for s in STORES if s["country_code"] == "MYS"]
SG_STORES = [s for s in STORES if s["country_code"] == "SGP"]

# ─── Tender mix per country ────────────────────────────────────
# Weights add to 1.0. Reflects realistic retail mix.
MY_TENDER_MIX = [
    ("CASH",       0.22),
    ("VISA",       0.20),
    ("MASTERCARD", 0.15),
    ("TNG",        0.16),
    ("GRABPAY_MY", 0.07),
    ("BOOST",      0.05),
    ("DUITNOW_MY", 0.06),
    ("MYDEBIT",    0.04),
    ("AMEX",       0.02),
    ("UNIONPAY",   0.01),
    ("VOUCHER",    0.01),
    ("BNPL_ATOME_MY",0.01),
]
SG_TENDER_MIX = [
    ("PAYNOW",     0.28),
    ("VISA",       0.22),
    ("NETS",       0.18),
    ("MASTERCARD", 0.12),
    ("GRABPAY_SG", 0.07),
    ("FAVE",       0.04),
    ("AMEX",       0.03),
    ("UNIONPAY",   0.02),
    ("CASH",       0.02),
    ("VOUCHER",    0.01),
    ("BNPL_ATOME_SG",0.01),
]

def pick_tender(country):
    mix = MY_TENDER_MIX if country == "MYS" else SG_TENDER_MIX
    codes = [m[0] for m in mix]
    weights = [m[1] for m in mix]
    return random.choices(codes, weights=weights, k=1)[0]

# ─── Daily volume multipliers ──────────────────────────────────
def volume_mult(d):
    """Returns a multiplier for daily transaction volume."""
    m = 1.0
    # Weekend boost
    if d.weekday() >= 5:
        m *= 1.35
    # Monthly pattern
    month_mod = {10:1.05, 11:1.25, 12:1.45, 1:0.85, 2:0.95, 3:1.00}
    m *= month_mod.get(d.month, 1.0)
    # Public holiday boost (shopping day)
    key_my = ("MYS", d.isoformat())
    key_sg = ("SGP", d.isoformat())
    if key_my in HOLIDAYS or key_sg in HOLIDAYS:
        m *= 1.20
    # Nov 11 / Nov sale event (11.11)
    if d.month == 11 and d.day == 11:
        m *= 1.80
    # Dec 12 (12.12 sale)
    if d.month == 12 and d.day == 12:
        m *= 1.60
    # Christmas Eve / Christmas
    if d.month == 12 and d.day in (24, 25, 26):
        m *= 1.50
    # CNY shopping rush (5 days before CNY)
    cny = date(2025, 1, 29)
    days_to_cny = (cny - d).days
    if 0 <= days_to_cny <= 5:
        m *= 1.40
    return m

def avg_basket(store, d):
    """Average transaction value in local currency."""
    base = {
        "FLAGSHIP": {"MYS": 580, "SGP": 195},
        "STANDARD": {"MYS": 320, "SGP": 120},
    }[store["store_type"]][store["country_code"]]
    # Dec premium
    if d.month == 12:
        base *= 1.15
    # slight random store variation
    base *= random.uniform(0.85, 1.15)
    return base

def base_txn_count(store):
    return {"FLAGSHIP":{"MYS":90,"SGP":85},"STANDARD":{"MYS":55,"SGP":50}
            }[store["store_type"]][store["country_code"]]

# ─── Tax calculation ──────────────────────────────────────────
TAX_RATES = {"MYS": 0.06, "SGP": 0.09}

def calc_tax(net_amount, country):
    r = TAX_RATES[country]
    tax = Decimal(str(net_amount)) * Decimal(str(r))
    return float(tax.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

# ─── Promo codes ──────────────────────────────────────────────
MY_PROMOS = [f"PROMO-MY-{y}-{n:03d}" for y in [2024,2025] for n in range(1,25)]
SG_PROMOS = [f"PROMO-SG-{y}-{n:03d}" for y in [2024,2025] for n in range(1,20)]
# These promos are deliberately NOT mapped in SAP (triggers RC-003)
UNMAPPED_PROMOS_MY = {"PROMO-MY-2024-015", "PROMO-MY-2024-022", "PROMO-MY-2025-003"}
UNMAPPED_PROMOS_SG = {"PROMO-SG-2024-011", "PROMO-SG-2025-002"}

# ─── Employee pools ───────────────────────────────────────────
MY_CASHIERS  = [f"EMP-MY-{i:05d}" for i in range(20100, 20160)]
SG_CASHIERS  = [f"EMP-SG-{i:05d}" for i in range(10080, 10130)]
MY_SUPERS    = [f"EMP-MY-{i:05d}" for i in range(10010, 10030)]
SG_SUPERS    = [f"EMP-SG-{i:05d}" for i in range(10040, 10060)]

# ─── Injected break flags ────────────────────────────────────
DQ_FLAGS = {
    "CLEAN":             "No issue",
    "PROMO_UNMAPPED":    "RC-003: Promo not mapped in SAP — will cause POS-SAP variance",
    "SAP_BATCH_FAIL":    "RC-001: SAP batch posting failed — no SAP document",
    "DUPLICATE_TXN":     "RC-005: Duplicate POS transaction",
    "ROUNDING_DIFF":     "RC-007: Rounding difference <0.05 between POS and SAP",
    "RETURN_NOT_REV":    "RC-006: Return not reversed in SAP",
    "WRONG_PERIOD":      "RC-008: Will post to wrong SAP period",
    "NULL_STORE":        "DQ: store_id is null — data quality issue",
    "WRONG_COUNTRY":     "DQ: country_code mismatch — ETL routing error",
    "NEG_TENDER":        "DQ: negative tender amount on non-return transaction",
    "FUTURE_DATE":       "DQ: business_date is in the future — terminal clock error",
    "WRONG_TAX":         "RC-013: Wrong tax rate applied (SST vs GST confusion)",
}

# ─── SAP document number sequences ───────────────────────────
sap_seq = {"MY01": 1800000000, "SG01": 2800000000}
def next_sap_doc(company_code):
    sap_seq[company_code] += 1
    return str(sap_seq[company_code])

# ─────────────────────────────────────────────────────────────────────────────
# MAIN GENERATION LOOP
# ─────────────────────────────────────────────────────────────────────────────

print("Generating POS transaction data...")

hdr_rows   = []   # pos_transaction_header
tndr_rows  = []   # pos_tender
eod_rows   = []   # pos_eod_summary

START_DATE = date(2024, 10, 1)
END_DATE   = date(2025, 3, 31)

# Track EOD summaries per (store, date) to detect double-submission DQ issue
eod_submitted = set()

# Stores with recurring issues (systemic — for trend page to catch)
PROBLEM_STORES = {"MY-STR-004", "SG-STR-003"}  # will have more exceptions

d = START_DATE
while d <= END_DATE:
    # ── batch-level break: some stores have SAP batch fail for the day
    batch_fail_stores = set()
    if random.random() < 0.008:  # ~0.8% of store-days
        fail_store = random.choice(STORES)["store_id"]
        batch_fail_stores.add(fail_store)

    for store in STORES:
        country   = store["country_code"]
        store_id  = store["store_id"]
        company   = "MY01" if country == "MYS" else "SG01"
        currency  = "MYR"  if country == "MYS" else "SGD"
        num_terms = int(store["num_terminals"])
        cashiers  = MY_CASHIERS if country == "MYS" else SG_CASHIERS
        supers    = MY_SUPERS   if country == "MYS" else SG_SUPERS
        promos    = MY_PROMOS   if country == "MYS" else SG_PROMOS
        bad_promos= UNMAPPED_PROMOS_MY if country == "MYS" else UNMAPPED_PROMOS_SG
        tax_rate  = TAX_RATES[country]
        tax_code  = "SR-6" if country == "MYS" else "SR"

        vmult = volume_mult(d)
        base  = base_txn_count(store)
        n_txns = max(5, int(base * vmult * random.uniform(0.88, 1.12)))

        # Problem stores get extra variance on some days
        if store_id in PROBLEM_STORES:
            n_txns = max(5, int(n_txns * random.uniform(0.95, 1.05)))

        # ── per-store EOD aggregates
        eod_net_sales     = 0.0
        eod_tax           = 0.0
        eod_cash_system   = 0.0
        eod_card_total    = 0.0
        eod_ewallet_total = 0.0
        eod_paynow        = 0.0
        eod_nets          = 0.0
        eod_duitnow       = 0.0

        for _ in range(n_txns):
            txn_id = f"TXN-{country[:2]}-{d.strftime('%Y%m%d')}-{str(uuid.uuid4())[:8].upper()}"
            terminal = f"TILL-{random.randint(1, num_terms):02d}"
            cashier  = random.choice(cashiers)

            # Transaction type
            txn_type = random.choices(
                ["SALE","RETURN","VOID"],
                weights=[0.93, 0.05, 0.02])[0]

            # ── Inject DQ/break flags ─────────────────────────
            dq_flag = "CLEAN"
            sap_doc = None
            sap_status = "POSTED"

            roll = random.random()
            is_problem_store = store_id in PROBLEM_STORES
            roll_mult = 2.0 if is_problem_store else 1.0  # problem stores 2× likely

            if store_id in batch_fail_stores:
                dq_flag = "SAP_BATCH_FAIL"
                sap_doc = None
                sap_status = "FAILED"
            elif roll < 0.030 * roll_mult:
                dq_flag = "PROMO_UNMAPPED"   # RC-003: will cause variance
            elif roll < 0.038 * roll_mult:
                dq_flag = "ROUNDING_DIFF"    # RC-007
            elif roll < 0.046 * roll_mult:
                dq_flag = "DUPLICATE_TXN"    # RC-005
            elif roll < 0.050 * roll_mult and txn_type == "RETURN":
                dq_flag = "RETURN_NOT_REV"   # RC-006
            elif roll < 0.052:
                dq_flag = "WRONG_PERIOD"     # RC-008
            elif roll < 0.053:
                dq_flag = "NULL_STORE"       # DQ
            elif roll < 0.0535:
                dq_flag = "WRONG_COUNTRY"    # DQ
            elif roll < 0.054:
                dq_flag = "NEG_TENDER"       # DQ
            elif roll < 0.0545:
                dq_flag = "FUTURE_DATE"      # DQ
            elif roll < 0.0555:
                dq_flag = "WRONG_TAX"        # RC-013

            # ── Amounts ──────────────────────────────────────
            basket = avg_basket(store, d)
            if txn_type == "SALE":
                gross = round(random.uniform(basket * 0.3, basket * 2.2), 2)
            elif txn_type == "RETURN":
                gross = -round(random.uniform(basket * 0.15, basket * 0.8), 2)
            else:
                gross = 0.00

            # Discount (25% of SALE transactions have a discount)
            discount = 0.0
            promo_code = ""
            if txn_type == "SALE" and random.random() < 0.25:
                disc_pct = random.choice([0.05, 0.10, 0.15, 0.20, 0.30])
                discount = round(abs(gross) * disc_pct, 2)
                # Decide if promo is an unmapped one (higher on problem stores)
                if dq_flag == "PROMO_UNMAPPED":
                    promo_code = random.choice(list(bad_promos))
                else:
                    promo_code = random.choice(promos)

            net = round(gross - discount, 2)

            # Tax
            if dq_flag == "WRONG_TAX":
                # Apply wrong country tax rate
                wrong_rate = 0.09 if country == "MYS" else 0.06
                tax_amt = round(abs(net) * wrong_rate, 2) * (1 if net >= 0 else -1)
            else:
                tax_amt = round(abs(net) * tax_rate, 2) * (1 if net >= 0 else -1)

            if dq_flag == "ROUNDING_DIFF":
                tax_amt += random.choice([-0.01, 0.01])

            total_tendered = round(net + tax_amt, 2)

            # Negative tender inject on non-return
            if dq_flag == "NEG_TENDER" and txn_type == "SALE":
                total_tendered = -abs(total_tendered)

            # Future date inject
            business_date = d
            if dq_flag == "FUTURE_DATE":
                business_date = d + timedelta(days=random.randint(1, 3))

            # Null store inject
            eff_store_id = "" if dq_flag == "NULL_STORE" else store_id

            # Wrong country inject
            eff_country = ("SGP" if country == "MYS" else "MYS") if dq_flag == "WRONG_COUNTRY" else country

            # SAP doc assignment
            if dq_flag == "SAP_BATCH_FAIL":
                sap_doc    = None
                sap_status = "FAILED"
            elif dq_flag == "DUPLICATE_TXN":
                sap_doc    = None   # duplicate — no SAP doc yet
                sap_status = "PENDING"
            elif txn_type == "VOID":
                sap_doc    = None
                sap_status = "PENDING"
            else:
                sap_doc    = next_sap_doc(company)
                sap_status = "POSTED"

            # Timestamp (UTC+8 stored as local; realistic store hours)
            hour     = random.randint(10, 21)
            minute   = random.randint(0, 59)
            second   = random.randint(0, 59)
            txn_dt   = f"{business_date}T{hour:02d}:{minute:02d}:{second:02d}+08:00"

            receipt = f"RCP-{store_id[-3:]}-{d.strftime('%Y%m%d')}-{txn_id[-6:]}"

            hdr_rows.append({
                "pos_txn_id":        txn_id,
                "country_code":      eff_country,
                "store_id":          eff_store_id,
                "terminal_id":       terminal,
                "cashier_id":        cashier,
                "business_date":     business_date.isoformat(),
                "txn_datetime_utc":  txn_dt,
                "txn_type":          txn_type,
                "txn_status":        "CANCELLED" if txn_type == "VOID" else "COMPLETED",
                "gross_amount":      gross,
                "discount_amount":   discount,
                "net_amount":        net,
                "tax_code":          tax_code,
                "tax_amount_local":  tax_amt,
                "currency_code":     currency,
                "total_tendered":    total_tendered,
                "promo_code":        promo_code,
                "receipt_number":    receipt,
                "sap_company_code":  company,
                "sap_doc_number":    sap_doc or "",
                "sap_post_status":   sap_status,
                "_dq_flag":          dq_flag,
                "_dq_description":   DQ_FLAGS[dq_flag],
            })

            # ── Tender lines ──────────────────────────────────
            if txn_type == "VOID" or total_tendered == 0:
                continue

            tender_code = pick_tender(country)
            # Ensure tender is valid for country
            td = TENDERS.get(tender_code, {})
            td_country = td.get("country_code","BOTH")
            if td_country not in ("BOTH", country):
                tender_code = "CASH"  # fallback

            tndr_id   = f"TNDR-{txn_id[-10:]}-01"
            paynow_r  = ""
            duitnow_r = ""
            nets_r    = ""
            card_l4   = ""
            card_type = ""
            approval  = ""
            rrn_val   = ""
            batch_ref = f"BATCH-{store_id[-3:]}-{tender_code[:3]}-{d.strftime('%Y%m%d')}-001"

            if tender_code == "PAYNOW":
                paynow_r = f"PAYNOW-{d.strftime('%Y%m%d')}-{random.randint(1000000,9999999)}"
            elif tender_code == "DUITNOW_MY":
                duitnow_r = f"DUITNOW-{d.strftime('%Y%m%d')}-{random.randint(100000,999999)}"
            elif tender_code == "NETS":
                nets_r = f"NETS-{random.randint(100000000000, 999999999999)}"
            elif TENDERS.get(tender_code, {}).get("is_card","0") == "1":
                card_l4   = f"{random.randint(1000,9999)}"
                card_type = tender_code
                approval  = f"AUTH{random.randint(100000,999999)}"
                rrn_val   = f"{d.strftime('%Y%m%d')}{random.randint(1000000000, 9999999999)}"

            ewallet_prov = ""
            if tender_code in ("TNG","GRABPAY_MY","BOOST","DUITNOW_MY",
                               "PAYNOW","GRABPAY_SG","FAVE"):
                ewallet_prov = tender_code

            tndr_rows.append({
                "pos_tender_id":    tndr_id,
                "pos_txn_id":       txn_id,
                "country_code":     country,
                "tender_code":      tender_code,
                "tender_amount":    abs(total_tendered),
                "currency_code":    currency,
                "ewallet_provider": ewallet_prov,
                "paynow_ref":       paynow_r,
                "duitnow_ref":      duitnow_r,
                "nets_ref":         nets_r,
                "card_last4":       card_l4,
                "card_type":        card_type,
                "approval_code":    approval,
                "rrn":              rrn_val,
                "settlement_batch": batch_ref,
                "business_date":    d.isoformat(),
                "_dq_flag":         dq_flag,
            })

            # ── EOD aggregates (only clean + posted rows)
            if txn_type in ("SALE","RETURN") and dq_flag not in ("NULL_STORE","WRONG_COUNTRY","FUTURE_DATE"):
                eod_net_sales += net
                eod_tax       += tax_amt
                if tender_code == "CASH":
                    eod_cash_system += abs(total_tendered)
                elif TENDERS.get(tender_code, {}).get("is_card","0") == "1":
                    eod_card_total  += abs(total_tendered)
                else:
                    eod_ewallet_total += abs(total_tendered)
                if tender_code == "PAYNOW":
                    eod_paynow  += abs(total_tendered)
                if tender_code == "NETS":
                    eod_nets    += abs(total_tendered)
                if tender_code == "DUITNOW_MY":
                    eod_duitnow += abs(total_tendered)

        # ── EOD Summary row for this store+date ───────────────
        # Cash variance: physical count differs slightly from system
        cash_variance = round(random.uniform(-30, 30), 2)
        if random.random() < 0.15:  # occasional larger variance
            cash_variance = round(random.uniform(-120, -50) if random.random() < 0.5
                                   else random.uniform(50, 120), 2)

        supervisor = random.choice(supers)

        eod_id = f"EOD-{country[:2]}-{store_id[-3:]}-T01-{d.strftime('%Y%m%d')}"

        # ── DQ: Duplicate EOD submission ~0.1% of store-days
        sap_xfer = "TRANSFERRED"
        eod_dq   = "CLEAN"
        if (store_id, d.isoformat()) in eod_submitted:
            eod_dq = "DQ-DUPLICATE_EOD"
        else:
            eod_submitted.add((store_id, d.isoformat()))
        if store_id in batch_fail_stores:
            sap_xfer = "FAILED"
            eod_dq   = "SAP_BATCH_FAIL"
        # Randomly inject ~0.1% duplicate EODs
        if random.random() < 0.001:
            eod_submitted.discard((store_id, d.isoformat()))  # allow dup next loop

        eod_rows.append({
            "eod_summary_id":     eod_id,
            "country_code":       country,
            "store_id":           store_id,
            "terminal_id":        "TILL-ALL",
            "business_date":      d.isoformat(),
            "fiscal_period_key":  f"{country}-{d.year}-{d.month:02d}",
            "currency_code":      currency,
            "net_sales":          round(eod_net_sales, 2),
            "tax_collected":      round(eod_tax, 2),
            "cash_variance":      cash_variance,
            "paynow_total":       round(eod_paynow, 2),
            "duitnow_total":      round(eod_duitnow, 2),
            "nets_total":         round(eod_nets, 2),
            "sap_transfer_status":sap_xfer,
            "supervisor_id":      supervisor,
            "_dq_flag":           eod_dq,
        })

    d += timedelta(days=1)

# ─── Write CSVs ───────────────────────────────────────────────
def write_csv(name, rows, fieldnames):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {name:50s}  ({len(rows):>8,} rows)")

write_csv("pos_transaction_header.csv", hdr_rows, [
    "pos_txn_id","country_code","store_id","terminal_id","cashier_id",
    "business_date","txn_datetime_utc","txn_type","txn_status",
    "gross_amount","discount_amount","net_amount","tax_code","tax_amount_local",
    "currency_code","total_tendered","promo_code","receipt_number",
    "sap_company_code","sap_doc_number","sap_post_status",
    "_dq_flag","_dq_description"])

write_csv("pos_tender.csv", tndr_rows, [
    "pos_tender_id","pos_txn_id","country_code","tender_code","tender_amount",
    "currency_code","ewallet_provider","paynow_ref","duitnow_ref","nets_ref",
    "card_last4","card_type","approval_code","rrn","settlement_batch",
    "business_date","_dq_flag"])

write_csv("pos_eod_summary.csv", eod_rows, [
    "eod_summary_id","country_code","store_id","terminal_id","business_date",
    "fiscal_period_key","currency_code","net_sales","tax_collected","cash_variance",
    "paynow_total","duitnow_total","nets_total","sap_transfer_status","supervisor_id",
    "_dq_flag"])

print("\nPOS transaction tables complete.")