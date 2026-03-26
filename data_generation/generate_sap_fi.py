"""
generate_data/generate_sap_fi.py
================================
Generates SAP FI tables derived from POS data:
  sap_fi_document.csv   (BKPF)
  sap_fi_lineitem.csv   (BSEG)

SAP posting rules:
  - POSTED POS txns → SAP document created
  - FAILED / PENDING → no SAP document
  - Some additional SAP-side breaks injected:
      * Wrong period posting (RC-008)
      * Manual posting with wrong amount (RC-004)
      * Reversed documents (stblg populated)
      * Promo unmapped → SAP posts gross without discount applied
"""

import csv, os, random
from datetime import date, timedelta

random.seed(2025)
OUT = os.path.join(os.path.dirname(__file__), "output")

def load_csv(filename):
    path = os.path.join(OUT, filename)
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows

def write_csv(name, rows, fieldnames):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {name:50s}  ({len(rows):>8,} rows)")

print("Loading POS transaction header...")
pos_txns = load_csv("pos_transaction_header.csv")

# ── GL account mapping per company ───────────────────────────
GL_MAP = {
    "MY01": {
        "sales":    "0040000001",  # Sales Revenue MY
        "tax":      "0021300001",  # SST Payable MY
        "clearing": "0011000010",  # POS Clearing MY
        "discount": "0050000001",  # Discount Expense MY
        "cash":     "0011000001",  # Cash MY
    },
    "SG01": {
        "sales":    "0040000002",  # Sales Revenue SG
        "tax":      "0021200001",  # GST Payable SG
        "clearing": "0011000020",  # POS Clearing SG
        "discount": "0050000002",  # Discount Expense SG
        "cash":     "0011000002",  # Cash SG
    },
}

# ── Batch posting users ───────────────────────────────────────
BATCH_USERS = {"MY01": "BATCH_POS_MY", "SG01": "BATCH_POS_SG"}
MANUAL_USERS_MY = ["FNCMY001","FNCMY002","FNCMY003"]
MANUAL_USERS_SG = ["FNCS G001","FNCSG002","FNCSG003"]

# ── Document type mapping ─────────────────────────────────────
DOC_TYPES = {"SALE": "RV", "RETURN": "RV", "VOID": "SA"}

doc_rows  = []  # BKPF
line_rows = []  # BSEG
belnr_seq = {"MY01": 1800000000, "SG01": 2800000000}

# Track reversal pairs
to_reverse = []

for txn in pos_txns:
    # Only process COMPLETED transactions with a SAP doc number
    if txn["txn_status"] != "COMPLETED":
        continue
    if not txn.get("sap_doc_number"):
        continue
    # Skip DQ-only issues that don't affect SAP posting
    dq = txn["_dq_flag"]
    if dq in ("SAP_BATCH_FAIL", "NULL_STORE", "WRONG_COUNTRY", "FUTURE_DATE"):
        continue

    company   = txn["sap_company_code"]
    currency  = txn["currency_code"]
    belnr     = txn["sap_doc_number"]
    bldat     = txn["business_date"]
    country   = txn["country_code"]

    # Wrong period: post to prior month end
    if dq == "WRONG_PERIOD":
        d_obj = date.fromisoformat(bldat)
        # Post to prior month 28th (always safe, any month)
        if d_obj.month == 1:
            budat = date(d_obj.year - 1, 12, 28).isoformat()
        else:
            budat = date(d_obj.year, d_obj.month - 1, 28).isoformat()
    else:
        budat = bldat

    # Posting amount — promo_unmapped means SAP posts gross (discount not deducted)
    net_amount = float(txn["net_amount"])
    gross_amount = float(txn["gross_amount"])
    discount    = float(txn["discount_amount"])
    tax_amount  = float(txn["tax_amount_local"])

    if dq == "PROMO_UNMAPPED" and discount > 0:
        # SAP posts gross (no discount deducted) → POS shows net (discounted)
        sap_net_amount = round(gross_amount, 2)   # SAP records gross
        sap_tax        = round(abs(gross_amount) * (0.06 if company == "MY01" else 0.09), 2)
    else:
        sap_net_amount = net_amount
        sap_tax        = tax_amount

    # Manual posting amount error (RC-004) — ~0.3% additional
    manual_error = False
    if random.random() < 0.003:
        manual_error = True
        sap_net_amount = round(sap_net_amount * random.uniform(0.95, 1.05), 2)

    blart = DOC_TYPES.get(txn["txn_type"], "RV")
    usnam = MANUAL_USERS_MY[0] if manual_error else BATCH_USERS.get(company, "BATCH_POS")

    # Tax country
    tax_country = "MY" if company == "MY01" else "SG"

    # Reversal flag — Return transactions sometimes get reversed
    stblg = ""
    bstat = " "

    doc_rows.append({
        "belnr":          belnr,
        "bukrs":          company,
        "gjahr":          bldat[:4],
        "country_code":   country,
        "blart":          blart,
        "bldat":          bldat,
        "budat":          budat,
        "waers":          currency,
        "xblnr":          txn["pos_txn_id"],
        "usnam":          usnam,
        "bstat":          bstat,
        "stblg":          stblg,
        "tax_country":    tax_country,
        "pos_txn_id":     txn["pos_txn_id"],
        "_sap_net_amount":sap_net_amount,   # carry through to line items
        "_sap_tax":       sap_tax,
        "_dq_flag":       dq,
        "_manual_error":  "Y" if manual_error else "N",
    })

    # Some RETURN docs flagged for potential reversal tracking
    if txn["txn_type"] == "RETURN" and random.random() < 0.08:
        to_reverse.append((belnr, company, bldat[:4], country, currency, bldat))

# ── Generate line items (BSEG) ────────────────────────────────
line_seq = 0
for doc in doc_rows:
    company   = doc["bukrs"]
    belnr     = doc["belnr"]
    gjahr     = doc["gjahr"]
    currency  = doc["waers"]
    gl        = GL_MAP[company]
    net_amt   = float(doc["_sap_net_amount"])
    tax_amt   = float(doc["_sap_tax"])
    dq        = doc["_dq_flag"]

    sign = 1 if net_amt >= 0 else -1
    abs_net = abs(net_amt)
    abs_tax = abs(tax_amt)

    # Line 1: Debit POS Clearing (or Credit on return)
    line_seq += 1
    line_rows.append({
        "belnr":          belnr,
        "bukrs":          company,
        "gjahr":          gjahr,
        "buzei":          "001",
        "hkont":          gl["clearing"],
        "shkzg":          "S" if sign > 0 else "H",
        "dmbtr":          round(abs_net + abs_tax, 2),
        "hwae":           currency,
        "augbl":          "",
        "augdt":          "",
        "mwskz":          "",
        "kostl":          "",
        "prctr":          "",
        "sgtxt":          f"POS clearing {doc['pos_txn_id']}",
        "_dq_flag":       dq,
    })
    # Line 2: Credit Sales Revenue
    line_seq += 1
    line_rows.append({
        "belnr":          belnr,
        "bukrs":          company,
        "gjahr":          gjahr,
        "buzei":          "002",
        "hkont":          gl["sales"],
        "shkzg":          "H" if sign > 0 else "S",
        "dmbtr":          abs_net,
        "hwae":           currency,
        "augbl":          "",
        "augdt":          "",
        "mwskz":          "S6" if company == "MY01" else "SR",
        "kostl":          "",
        "prctr":          "",
        "sgtxt":          "Sales Revenue",
        "_dq_flag":       dq,
    })
    # Line 3: Credit Tax Payable (if tax > 0)
    if abs_tax > 0:
        line_seq += 1
        line_rows.append({
            "belnr":      belnr,
            "bukrs":      company,
            "gjahr":      gjahr,
            "buzei":      "003",
            "hkont":      gl["tax"],
            "shkzg":      "H" if sign > 0 else "S",
            "dmbtr":      abs_tax,
            "hwae":       currency,
            "augbl":      "",
            "augdt":      "",
            "mwskz":      "S6" if company == "MY01" else "SR",
            "kostl":      "",
            "prctr":      "",
            "sgtxt":      "SST / GST payable",
            "_dq_flag":   dq,
        })

# Remove internal carry-through columns before writing
doc_write = []
for d in doc_rows:
    row = {k: v for k, v in d.items() if not k.startswith("_sap")}
    doc_write.append(row)

write_csv("sap_fi_document.csv", doc_write, [
    "belnr","bukrs","gjahr","country_code","blart","bldat","budat","waers",
    "xblnr","usnam","bstat","stblg","tax_country","pos_txn_id",
    "_dq_flag","_manual_error"])

write_csv("sap_fi_lineitem.csv", line_rows, [
    "belnr","bukrs","gjahr","buzei","hkont","shkzg","dmbtr","hwae",
    "augbl","augdt","mwskz","kostl","prctr","sgtxt","_dq_flag"])

print(f"\nSAP FI tables complete. {len(doc_rows):,} documents, {len(line_rows):,} line items.")