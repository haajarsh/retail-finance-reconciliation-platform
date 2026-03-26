"""
generate_data/generate_dimensions.py
====================================
Generates all dimension/reference tables for the
NoveoMart POS–SAP–Bank Reconciliation portfolio project.

Tables produced:
  dim_country.csv
  dim_currency.csv
  dim_fiscal_calendar.csv
  dim_tax_code.csv
  dim_bank.csv
  dim_store.csv
  dim_tender_type.csv
  dim_root_cause.csv

Countries: Singapore (SGP) + Malaysia (MYS)
Period   : 2024-10-01 → 2025-03-31  (6 months)
"""

import csv, os, random
from datetime import date, timedelta

OUT = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUT, exist_ok=True)

def write_csv(name, rows, fieldnames):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {name:45s}  ({len(rows):>5} rows)")
    return path

# ─────────────────────────────────────────────────────────────
# 1. DIM_COUNTRY
# ─────────────────────────────────────────────────────────────
dim_country = [
    {
        "country_code": "MYS",
        "country_code_iso2": "MY",
        "country_name": "Malaysia",
        "sap_company_code": "MY01",
        "base_currency": "MYR",
        "group_currency": "USD",
        "tax_system": "SST",
        "tax_rate_standard": 6.00,
        "fiscal_year_end_month": 12,
        "timezone": "UTC+8",
        "settlement_lag_days": 1,
        "variance_threshold_t2": 50.00,
        "variance_threshold_t3": 200.00,
        "variance_threshold_t4": 1000.00,
        "regulatory_body": "BNM",
        "data_privacy_law": "PDPA Malaysia 2010",
        "dominant_ewallet": "Touch n Go, GrabPay MY, Boost, ShopeePay",
        "bank_transfer_rail": "IBG / DuitNow",
        "is_active": "Y",
    },
    {
        "country_code": "SGP",
        "country_code_iso2": "SG",
        "country_name": "Singapore",
        "sap_company_code": "SG01",
        "base_currency": "SGD",
        "group_currency": "USD",
        "tax_system": "GST",
        "tax_rate_standard": 9.00,
        "fiscal_year_end_month": 12,
        "timezone": "UTC+8",
        "settlement_lag_days": 1,
        "variance_threshold_t2": 20.00,
        "variance_threshold_t3": 100.00,
        "variance_threshold_t4": 500.00,
        "regulatory_body": "MAS",
        "data_privacy_law": "PDPA Singapore 2012 (amended 2021)",
        "dominant_ewallet": "GrabPay SG, PayNow, FAVE, Singtel Dash",
        "bank_transfer_rail": "FAST / PayNow",
        "is_active": "Y",
    },
]
write_csv("dim_country.csv", dim_country,
          ["country_code","country_code_iso2","country_name","sap_company_code",
           "base_currency","group_currency","tax_system","tax_rate_standard",
           "fiscal_year_end_month","timezone","settlement_lag_days",
           "variance_threshold_t2","variance_threshold_t3","variance_threshold_t4",
           "regulatory_body","data_privacy_law","dominant_ewallet",
           "bank_transfer_rail","is_active"])

# ─────────────────────────────────────────────────────────────
# 2. DIM_CURRENCY  (daily rates Oct 2024 – Mar 2025)
# ─────────────────────────────────────────────────────────────
# Base rates with gentle daily drift
BASE_RATES = {
    "MYR": {"to_usd": 0.2128, "to_sgd": 0.2865, "source": "BNM"},
    "SGD": {"to_usd": 0.7425, "to_sgd": 1.0000, "source": "MAS"},
    "USD": {"to_usd": 1.0000, "to_sgd": 1.3470, "source": "FED"},
}
random.seed(42)
currency_rows = []
start = date(2024, 10, 1)
end   = date(2025, 3, 31)
d = start
while d <= end:
    for ccy, meta in BASE_RATES.items():
        drift = 1 + random.uniform(-0.003, 0.003)
        currency_rows.append({
            "currency_code":      ccy,
            "currency_name":      {"MYR":"Malaysian Ringgit","SGD":"Singapore Dollar","USD":"US Dollar"}[ccy],
            "currency_symbol":    {"MYR":"RM","SGD":"S$","USD":"US$"}[ccy],
            "decimal_places":     2,
            "rate_date":          d.isoformat(),
            "rate_to_usd":        round(meta["to_usd"] * drift, 6),
            "rate_to_sgd":        round(meta["to_sgd"] * drift, 6),
            "rate_source":        meta["source"],
            "is_group_currency":  "Y" if ccy == "USD" else "N",
        })
    d += timedelta(days=1)
write_csv("dim_currency.csv", currency_rows,
          ["currency_code","currency_name","currency_symbol","decimal_places",
           "rate_date","rate_to_usd","rate_to_sgd","rate_source","is_group_currency"])

# ─────────────────────────────────────────────────────────────
# 3. DIM_FISCAL_CALENDAR  (one row per calendar date per country)
# ─────────────────────────────────────────────────────────────
# Public holidays for Oct 2024 – Mar 2025
MY_HOLIDAYS = {
    date(2024,10,31): "Deepavali",
    date(2024,12,25): "Christmas Day",
    date(2025, 1, 1): "New Year's Day",
    date(2025, 1,29): "Chinese New Year",
    date(2025, 1,30): "Chinese New Year (2nd day)",
    date(2025, 2, 1): "Federal Territory Day",
    date(2025, 3,31): "Hari Raya Aidilfitri",
}
SG_HOLIDAYS = {
    date(2024,10,31): "Deepavali",
    date(2024,12,25): "Christmas Day",
    date(2025, 1, 1): "New Year's Day",
    date(2025, 1,29): "Chinese New Year",
    date(2025, 1,30): "Chinese New Year (2nd day)",
}

fiscal_rows = []
d = start
while d <= end:
    for country, holidays in [("MYS", MY_HOLIDAYS), ("SGP", SG_HOLIDAYS)]:
        period_no = d.month
        fy        = d.year
        fq        = f"Q{(d.month - 1) // 3 + 1}"
        p_start   = date(d.year, d.month, 1)
        if d.month == 12:
            p_end = date(d.year, 12, 31)
        else:
            p_end = date(d.year, d.month + 1, 1) - timedelta(days=1)
        is_wday   = d.weekday() < 5  # Mon-Fri
        hol_name  = holidays.get(d)
        working   = "Y" if (is_wday and not hol_name) else "N"
        fiscal_rows.append({
            "fiscal_period_key":   f"{country}-{d.year}-{d.month:02d}",
            "country_code":        country,
            "date_key":            d.isoformat(),
            "fiscal_year":         fy,
            "fiscal_period":       period_no,
            "fiscal_quarter":      fq,
            "period_start_date":   p_start.isoformat(),
            "period_end_date":     p_end.isoformat(),
            "is_period_open":      "N" if d < date(2025,3,1) else "Y",
            "is_year_end_period":  "Y" if d.month == 12 else "N",
            "working_day_flag":    working,
            "public_holiday_name": hol_name or "",
        })
    d += timedelta(days=1)
write_csv("dim_fiscal_calendar.csv", fiscal_rows,
          ["fiscal_period_key","country_code","date_key","fiscal_year",
           "fiscal_period","fiscal_quarter","period_start_date","period_end_date",
           "is_period_open","is_year_end_period","working_day_flag","public_holiday_name"])

# ─────────────────────────────────────────────────────────────
# 4. DIM_TAX_CODE
# ─────────────────────────────────────────────────────────────
dim_tax = [
    # Malaysia SST codes
    {"tax_code_key":"MYS-SR-6",  "country_code":"MYS","tax_code":"SR-6", "tax_type":"SST","tax_description":"SST Standard Rated Service — 6%",             "tax_rate_pct":6.00, "effective_date":"2024-10-01","end_date":"","sap_tax_code":"S6","is_exempt":"N","gl_account_tax":"0021300001","filing_frequency":"Bi-monthly","tax_authority":"Royal Malaysian Customs Department"},
    {"tax_code_key":"MYS-SR-8",  "country_code":"MYS","tax_code":"SR-8", "tax_type":"SST","tax_description":"SST Standard Rated Goods — 8%",               "tax_rate_pct":8.00, "effective_date":"2024-10-01","end_date":"","sap_tax_code":"S8","is_exempt":"N","gl_account_tax":"0021300002","filing_frequency":"Bi-monthly","tax_authority":"Royal Malaysian Customs Department"},
    {"tax_code_key":"MYS-ZR",    "country_code":"MYS","tax_code":"ZR",   "tax_type":"SST","tax_description":"SST Zero Rated / Exempt Supply",               "tax_rate_pct":0.00, "effective_date":"2024-10-01","end_date":"","sap_tax_code":"ZR","is_exempt":"Y","gl_account_tax":"","filing_frequency":"Bi-monthly","tax_authority":"Royal Malaysian Customs Department"},
    # Singapore GST codes
    {"tax_code_key":"SGP-SR",    "country_code":"SGP","tax_code":"SR",   "tax_type":"GST","tax_description":"GST Standard Rated Supply — 9%",               "tax_rate_pct":9.00, "effective_date":"2024-01-01","end_date":"","sap_tax_code":"SR","is_exempt":"N","gl_account_tax":"0021200001","filing_frequency":"Quarterly","tax_authority":"Inland Revenue Authority of Singapore (IRAS)"},
    {"tax_code_key":"SGP-ZR",    "country_code":"SGP","tax_code":"ZR",   "tax_type":"GST","tax_description":"GST Zero Rated Supply",                        "tax_rate_pct":0.00, "effective_date":"2024-01-01","end_date":"","sap_tax_code":"ZR","is_exempt":"Y","gl_account_tax":"","filing_frequency":"Quarterly","tax_authority":"Inland Revenue Authority of Singapore (IRAS)"},
    {"tax_code_key":"SGP-ES",    "country_code":"SGP","tax_code":"ES",   "tax_type":"GST","tax_description":"GST Exempt Supply — Financial / Residential",   "tax_rate_pct":0.00, "effective_date":"2024-01-01","end_date":"","sap_tax_code":"ES","is_exempt":"Y","gl_account_tax":"","filing_frequency":"Quarterly","tax_authority":"Inland Revenue Authority of Singapore (IRAS)"},
]
write_csv("dim_tax_code.csv", dim_tax,
          ["tax_code_key","country_code","tax_code","tax_type","tax_description",
           "tax_rate_pct","effective_date","end_date","sap_tax_code","is_exempt",
           "gl_account_tax","filing_frequency","tax_authority"])

# ─────────────────────────────────────────────────────────────
# 5. DIM_BANK
# ─────────────────────────────────────────────────────────────
dim_bank = [
    # Malaysia banks
    {"bank_code":"MAYB","country_code":"MYS","bank_name":"Malayan Banking Berhad","bank_short_name":"Maybank",      "swift_bic":"MBBEMYKL","bank_account_no":"514-100001-001","account_type":"COLLECTION","sap_house_bank":"MAYB1","sap_account_id":"MAIN1","statement_format":"MT940","transfer_rail":"IBG / DuitNow","settlement_cut_off":"17:00","contact_name":"Ahmad Farid bin Hamid","is_active":"Y"},
    {"bank_code":"CIMB","country_code":"MYS","bank_name":"CIMB Bank Berhad",      "bank_short_name":"CIMB Bank",   "swift_bic":"CIBBMYKL","bank_account_no":"800-200001-001","account_type":"COLLECTION","sap_house_bank":"CIMB1","sap_account_id":"MAIN1","statement_format":"MT940","transfer_rail":"IBG / DuitNow","settlement_cut_off":"17:00","contact_name":"Nurul Ain binti Rashid","is_active":"Y"},
    {"bank_code":"RHB", "country_code":"MYS","bank_name":"RHB Bank Berhad",       "bank_short_name":"RHB Bank",    "swift_bic":"RHBBMYKL","bank_account_no":"212-300001-001","account_type":"COLLECTION","sap_house_bank":"RHB01","sap_account_id":"MAIN1","statement_format":"MT940","transfer_rail":"IBG / DuitNow","settlement_cut_off":"17:00","contact_name":"Chong Wei Liang","is_active":"Y"},
    # Singapore banks
    {"bank_code":"DBS", "country_code":"SGP","bank_name":"DBS Bank Ltd",           "bank_short_name":"DBS",         "swift_bic":"DBSSSGSG","bank_account_no":"003-100001-01","account_type":"CURRENT","sap_house_bank":"DBS01","sap_account_id":"MAIN1","statement_format":"MT940","transfer_rail":"FAST / PayNow","settlement_cut_off":"17:00","contact_name":"Sarah Lim Hui Ying","is_active":"Y"},
    {"bank_code":"OCBC","country_code":"SGP","bank_name":"Oversea-Chinese Banking Corporation","bank_short_name":"OCBC","swift_bic":"OCBCSGSG","bank_account_no":"501-200001-01","account_type":"CURRENT","sap_house_bank":"OCBC1","sap_account_id":"MAIN1","statement_format":"MT940","transfer_rail":"FAST / PayNow","settlement_cut_off":"17:00","contact_name":"James Tan Keng Hwee","is_active":"Y"},
    {"bank_code":"UOB", "country_code":"SGP","bank_name":"United Overseas Bank Ltd","bank_short_name":"UOB",        "swift_bic":"UOVBSGSG","bank_account_no":"359-300001-01","account_type":"CURRENT","sap_house_bank":"UOB01","sap_account_id":"MAIN1","statement_format":"MT940","transfer_rail":"FAST / PayNow","settlement_cut_off":"17:00","contact_name":"Michelle Wong Siew Khim","is_active":"Y"},
]
write_csv("dim_bank.csv", dim_bank,
          ["bank_code","country_code","bank_name","bank_short_name","swift_bic",
           "bank_account_no","account_type","sap_house_bank","sap_account_id",
           "statement_format","transfer_rail","settlement_cut_off","contact_name","is_active"])

# ─────────────────────────────────────────────────────────────
# 6. DIM_STORE  (15 stores total: 8 MY + 7 SG)
# ─────────────────────────────────────────────────────────────
dim_store = [
    # Malaysia stores
    {"store_id":"MY-STR-001","country_code":"MYS","store_name":"NoveoMart KLCC",       "mall_name":"Suria KLCC",        "city":"Kuala Lumpur","state":"WP Kuala Lumpur","store_type":"FLAGSHIP","sap_profit_center":"PC-MY-001","sap_cost_center":"CC-MY-001","bank_account":"514-100001-001","bank_code":"MAYB","mas_merchant_id":"","tax_registration":"SST-MY-001001","store_manager":"Hafizuddin bin Kamarudin","open_date":"2018-03-15","is_active":"Y","floor_area_sqft":8500,"num_terminals":6},
    {"store_id":"MY-STR-002","country_code":"MYS","store_name":"NoveoMart Pavilion",   "mall_name":"Pavilion KL",       "city":"Kuala Lumpur","state":"WP Kuala Lumpur","store_type":"STANDARD","sap_profit_center":"PC-MY-002","sap_cost_center":"CC-MY-002","bank_account":"514-100001-001","bank_code":"MAYB","mas_merchant_id":"","tax_registration":"SST-MY-001002","store_manager":"Siti Norzaharah binti Ahmad","open_date":"2019-06-01","is_active":"Y","floor_area_sqft":5200,"num_terminals":4},
    {"store_id":"MY-STR-003","country_code":"MYS","store_name":"NoveoMart Mid Valley", "mall_name":"Mid Valley Megamall","city":"Kuala Lumpur","state":"WP Kuala Lumpur","store_type":"STANDARD","sap_profit_center":"PC-MY-003","sap_cost_center":"CC-MY-003","bank_account":"514-100001-001","bank_code":"MAYB","mas_merchant_id":"","tax_registration":"SST-MY-001003","store_manager":"Tan Beng Kiat","open_date":"2019-11-20","is_active":"Y","floor_area_sqft":4800,"num_terminals":4},
    {"store_id":"MY-STR-004","country_code":"MYS","store_name":"NoveoMart Sunway",     "mall_name":"Sunway Pyramid",    "city":"Subang Jaya","state":"Selangor","store_type":"STANDARD","sap_profit_center":"PC-MY-004","sap_cost_center":"CC-MY-004","bank_account":"800-200001-001","bank_code":"CIMB","mas_merchant_id":"","tax_registration":"SST-MY-001004","store_manager":"Rajendran a/l Muthu","open_date":"2020-02-14","is_active":"Y","floor_area_sqft":4200,"num_terminals":3},
    {"store_id":"MY-STR-005","country_code":"MYS","store_name":"NoveoMart IOI City",   "mall_name":"IOI City Mall",     "city":"Putrajaya","state":"WP Putrajaya","store_type":"STANDARD","sap_profit_center":"PC-MY-005","sap_cost_center":"CC-MY-005","bank_account":"800-200001-001","bank_code":"CIMB","mas_merchant_id":"","tax_registration":"SST-MY-001005","store_manager":"Nurul Hidayah binti Zainudin","open_date":"2021-05-10","is_active":"Y","floor_area_sqft":3600,"num_terminals":3},
    {"store_id":"MY-STR-006","country_code":"MYS","store_name":"NoveoMart 1 Utama",    "mall_name":"1 Utama Shopping Centre","city":"Petaling Jaya","state":"Selangor","store_type":"STANDARD","sap_profit_center":"PC-MY-006","sap_cost_center":"CC-MY-006","bank_account":"212-300001-001","bank_code":"RHB","mas_merchant_id":"","tax_registration":"SST-MY-001006","store_manager":"Lee Choon Wei","open_date":"2021-09-25","is_active":"Y","floor_area_sqft":3900,"num_terminals":3},
    {"store_id":"MY-STR-007","country_code":"MYS","store_name":"NoveoMart Gurney",     "mall_name":"Gurney Plaza",      "city":"George Town","state":"Penang","store_type":"STANDARD","sap_profit_center":"PC-MY-007","sap_cost_center":"CC-MY-007","bank_account":"212-300001-001","bank_code":"RHB","mas_merchant_id":"","tax_registration":"SST-MY-001007","store_manager":"Priya d/o Krishnamurthy","open_date":"2022-03-01","is_active":"Y","floor_area_sqft":3200,"num_terminals":3},
    {"store_id":"MY-STR-008","country_code":"MYS","store_name":"NoveoMart Paradigm",   "mall_name":"Paradigm Mall JB",  "city":"Johor Bahru","state":"Johor","store_type":"STANDARD","sap_profit_center":"PC-MY-008","sap_cost_center":"CC-MY-008","bank_account":"212-300001-001","bank_code":"RHB","mas_merchant_id":"","tax_registration":"SST-MY-001008","store_manager":"Mohammad Asyraf bin Othman","open_date":"2022-10-15","is_active":"Y","floor_area_sqft":2800,"num_terminals":2},
    # Singapore stores
    {"store_id":"SG-STR-001","country_code":"SGP","store_name":"NoveoMart ION Orchard","mall_name":"ION Orchard",       "city":"Singapore","state":"Central","store_type":"FLAGSHIP","sap_profit_center":"PC-SG-001","sap_cost_center":"CC-SG-001","bank_account":"003-100001-01","bank_code":"DBS","mas_merchant_id":"MAS-SG-NVM-001","tax_registration":"GST-SG-200100001A","store_manager":"Cynthia Ng Swee Lin","open_date":"2019-01-15","is_active":"Y","floor_area_sqft":7200,"num_terminals":5},
    {"store_id":"SG-STR-002","country_code":"SGP","store_name":"NoveoMart Vivocity",   "mall_name":"VivoCity",          "city":"Singapore","state":"South","store_type":"STANDARD","sap_profit_center":"PC-SG-002","sap_cost_center":"CC-SG-002","bank_account":"003-100001-01","bank_code":"DBS","mas_merchant_id":"MAS-SG-NVM-002","tax_registration":"GST-SG-200100001A","store_manager":"Benjamin Loh Zhi Hao","open_date":"2019-08-20","is_active":"Y","floor_area_sqft":4600,"num_terminals":4},
    {"store_id":"SG-STR-003","country_code":"SGP","store_name":"NoveoMart Bugis",      "mall_name":"Bugis Junction",    "city":"Singapore","state":"Central","store_type":"STANDARD","sap_profit_center":"PC-SG-003","sap_cost_center":"CC-SG-003","bank_account":"501-200001-01","bank_code":"OCBC","mas_merchant_id":"MAS-SG-NVM-003","tax_registration":"GST-SG-200100001A","store_manager":"Grace Tan Mei Ling","open_date":"2020-07-01","is_active":"Y","floor_area_sqft":3800,"num_terminals":3},
    {"store_id":"SG-STR-004","country_code":"SGP","store_name":"NoveoMart Tampines",   "mall_name":"Tampines Mall",     "city":"Singapore","state":"East","store_type":"STANDARD","sap_profit_center":"PC-SG-004","sap_cost_center":"CC-SG-004","bank_account":"501-200001-01","bank_code":"OCBC","mas_merchant_id":"MAS-SG-NVM-004","tax_registration":"GST-SG-200100001A","store_manager":"Kevin Ong Jian Ming","open_date":"2021-04-10","is_active":"Y","floor_area_sqft":3400,"num_terminals":3},
    {"store_id":"SG-STR-005","country_code":"SGP","store_name":"NoveoMart Jurong",     "mall_name":"JEM Shopping Mall", "city":"Singapore","state":"West","store_type":"STANDARD","sap_profit_center":"PC-SG-005","sap_cost_center":"CC-SG-005","bank_account":"359-300001-01","bank_code":"UOB","mas_merchant_id":"MAS-SG-NVM-005","tax_registration":"GST-SG-200100001A","store_manager":"Patricia Yeo Hwee Lian","open_date":"2021-11-30","is_active":"Y","floor_area_sqft":3100,"num_terminals":3},
    {"store_id":"SG-STR-006","country_code":"SGP","store_name":"NoveoMart Northpoint", "mall_name":"Northpoint City",   "city":"Singapore","state":"North","store_type":"STANDARD","sap_profit_center":"PC-SG-006","sap_cost_center":"CC-SG-006","bank_account":"359-300001-01","bank_code":"UOB","mas_merchant_id":"MAS-SG-NVM-006","tax_registration":"GST-SG-200100001A","store_manager":"Alvin Goh Chee Keong","open_date":"2022-06-15","is_active":"Y","floor_area_sqft":2700,"num_terminals":2},
    {"store_id":"SG-STR-007","country_code":"SGP","store_name":"NoveoMart Changi",     "mall_name":"Jewel Changi Airport","city":"Singapore","state":"East","store_type":"STANDARD","sap_profit_center":"PC-SG-007","sap_cost_center":"CC-SG-007","bank_account":"359-300001-01","bank_code":"UOB","mas_merchant_id":"MAS-SG-NVM-007","tax_registration":"GST-SG-200100001A","store_manager":"Stephanie Koh Pei Ling","open_date":"2023-01-20","is_active":"Y","floor_area_sqft":2400,"num_terminals":2},
]
write_csv("dim_store.csv", dim_store,
          ["store_id","country_code","store_name","mall_name","city","state",
           "store_type","sap_profit_center","sap_cost_center","bank_account",
           "bank_code","mas_merchant_id","tax_registration","store_manager",
           "open_date","is_active","floor_area_sqft","num_terminals"])

# ─────────────────────────────────────────────────────────────
# 7. DIM_TENDER_TYPE
# ─────────────────────────────────────────────────────────────
dim_tender = [
    # ── Universal
    {"tender_code":"CASH",          "country_code":"BOTH","tender_name":"Cash",                    "tender_category":"CASH",   "sap_gl_account":"0011000001","settlement_lag_days":0,"is_card":0,"is_ewallet":0,"interchange_rate_pct":0.00,"is_active":"Y","notes":"Cash. No settlement lag. Variance = cash over/short."},
    {"tender_code":"VISA",          "country_code":"BOTH","tender_name":"Visa Credit / Debit",     "tender_category":"CARD",   "sap_gl_account":"0011100001","settlement_lag_days":1,"is_card":1,"is_ewallet":0,"interchange_rate_pct":1.50,"is_active":"Y","notes":"T+1 settlement. Standard MDR ~1.5%."},
    {"tender_code":"MASTERCARD",    "country_code":"BOTH","tender_name":"Mastercard Credit / Debit","tender_category":"CARD",  "sap_gl_account":"0011100002","settlement_lag_days":1,"is_card":1,"is_ewallet":0,"interchange_rate_pct":1.50,"is_active":"Y","notes":"T+1 settlement."},
    {"tender_code":"AMEX",          "country_code":"BOTH","tender_name":"American Express",         "tender_category":"CARD",   "sap_gl_account":"0011100003","settlement_lag_days":2,"is_card":1,"is_ewallet":0,"interchange_rate_pct":2.80,"is_active":"Y","notes":"T+2 settlement. Higher MDR ~2.8%."},
    {"tender_code":"UNIONPAY",      "country_code":"BOTH","tender_name":"UnionPay",                 "tender_category":"CARD",   "sap_gl_account":"0011100004","settlement_lag_days":1,"is_card":1,"is_ewallet":0,"interchange_rate_pct":0.80,"is_active":"Y","notes":"Popular with tourist spend. Low MDR."},
    {"tender_code":"VOUCHER",       "country_code":"BOTH","tender_name":"Gift Voucher / Gift Card", "tender_category":"VOUCHER","sap_gl_account":"0011200001","settlement_lag_days":0,"is_card":0,"is_ewallet":0,"interchange_rate_pct":0.00,"is_active":"Y","notes":"Redeemed in-store. Contra to gift card liability GL."},
    # ── Malaysia specific
    {"tender_code":"TNG",           "country_code":"MYS", "tender_name":"Touch n Go eWallet",      "tender_category":"EWALLET","sap_gl_account":"0011300001","settlement_lag_days":1,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.70,"is_active":"Y","notes":"Most dominant MY e-wallet. T+1 settlement via TnG operator."},
    {"tender_code":"GRABPAY_MY",    "country_code":"MYS", "tender_name":"GrabPay Malaysia",         "tender_category":"EWALLET","sap_gl_account":"0011300002","settlement_lag_days":1,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.70,"is_active":"Y","notes":"Grab Pay MY entity — separate from SG."},
    {"tender_code":"BOOST",         "country_code":"MYS", "tender_name":"Boost eWallet",            "tender_category":"EWALLET","sap_gl_account":"0011300003","settlement_lag_days":1,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.70,"is_active":"Y","notes":"Axiata-owned MY e-wallet."},
    {"tender_code":"DUITNOW_MY",    "country_code":"MYS", "tender_name":"DuitNow QR",               "tender_category":"EWALLET","sap_gl_account":"0011300004","settlement_lag_days":1,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.00,"is_active":"Y","notes":"BNM-mandated zero-MDR QR standard. IBG transfer."},
    {"tender_code":"MYDEBIT",       "country_code":"MYS", "tender_name":"MyDebit",                  "tender_category":"CARD",   "sap_gl_account":"0011100005","settlement_lag_days":1,"is_card":1,"is_ewallet":0,"interchange_rate_pct":0.15,"is_active":"Y","notes":"Malaysian domestic debit scheme. Low MDR."},
    {"tender_code":"BNPL_ATOME_MY", "country_code":"MYS", "tender_name":"Atome BNPL Malaysia",      "tender_category":"BNPL",   "sap_gl_account":"0011400001","settlement_lag_days":2,"is_card":0,"is_ewallet":0,"interchange_rate_pct":3.50,"is_active":"Y","notes":"Buy Now Pay Later. T+2. Higher MDR. Separate merchant agreement."},
    # ── Singapore specific
    {"tender_code":"PAYNOW",        "country_code":"SGP", "tender_name":"PayNow / FAST",            "tender_category":"EWALLET","sap_gl_account":"0012300001","settlement_lag_days":0,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.00,"is_active":"Y","notes":"Real-time. FAST network. Zero MDR. Very dominant in SG."},
    {"tender_code":"NETS",          "country_code":"SGP", "tender_name":"NETS Debit",               "tender_category":"CARD",   "sap_gl_account":"0012100001","settlement_lag_days":1,"is_card":1,"is_ewallet":0,"interchange_rate_pct":0.55,"is_active":"Y","notes":"SG domestic debit. NETS operator settlement — separate from Visa/MC processor."},
    {"tender_code":"GRABPAY_SG",    "country_code":"SGP", "tender_name":"GrabPay Singapore",        "tender_category":"EWALLET","sap_gl_account":"0012300002","settlement_lag_days":1,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.70,"is_active":"Y","notes":"Grab Pay SG entity."},
    {"tender_code":"FAVE",          "country_code":"SGP", "tender_name":"Fave eWallet",             "tender_category":"EWALLET","sap_gl_account":"0012300003","settlement_lag_days":1,"is_card":0,"is_ewallet":1,"interchange_rate_pct":0.80,"is_active":"Y","notes":"Fave cashback platform. Popular in SG."},
    {"tender_code":"BNPL_ATOME_SG", "country_code":"SGP", "tender_name":"Atome BNPL Singapore",     "tender_category":"BNPL",   "sap_gl_account":"0012400001","settlement_lag_days":2,"is_card":0,"is_ewallet":0,"interchange_rate_pct":3.50,"is_active":"Y","notes":"SG Atome entity. T+2 settlement."},
]
write_csv("dim_tender_type.csv", dim_tender,
          ["tender_code","country_code","tender_name","tender_category",
           "sap_gl_account","settlement_lag_days","is_card","is_ewallet",
           "interchange_rate_pct","is_active","notes"])

# ─────────────────────────────────────────────────────────────
# 8. DIM_ROOT_CAUSE
# ─────────────────────────────────────────────────────────────
dim_rc = [
    {"root_cause_code":"RC-001","category":"SYSTEM",  "subcategory":"Batch Job",     "description":"SAP batch posting job failed or timed out",                        "responsible_team":"IT-SAP",    "typical_resolution_hrs":4, "is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-002","category":"SYSTEM",  "subcategory":"Integration",   "description":"POS-SAP integration feed not received (middleware timeout)",        "responsible_team":"IT-Middleware","typical_resolution_hrs":6,"is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-003","category":"CONFIG",  "subcategory":"Promo Mapping", "description":"Promotion code not configured in SAP pricing conditions",            "responsible_team":"IT-SAP",    "typical_resolution_hrs":8, "is_systemic":"Y","occurrence_count":0},
    {"root_cause_code":"RC-004","category":"PROCESS", "subcategory":"Manual Entry",  "description":"Manual SAP posting entered with incorrect amount",                   "responsible_team":"Finance",   "typical_resolution_hrs":2, "is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-005","category":"DATA",    "subcategory":"Duplicate",     "description":"Duplicate POS transaction transmitted to SAP",                       "responsible_team":"IT-POS",    "typical_resolution_hrs":3, "is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-006","category":"PROCESS", "subcategory":"Return",        "description":"POS return not correctly reversed in SAP",                           "responsible_team":"Finance",   "typical_resolution_hrs":2, "is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-007","category":"DATA",    "subcategory":"Rounding",      "description":"Rounding difference between POS (2dp) and SAP (2dp) due to tax calc","responsible_team":"IT-SAP",   "typical_resolution_hrs":1, "is_systemic":"Y","occurrence_count":0},
    {"root_cause_code":"RC-008","category":"PROCESS", "subcategory":"Period",        "description":"SAP posting in wrong fiscal period (prior period close issue)",       "responsible_team":"Finance",   "typical_resolution_hrs":4, "is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-009","category":"PAYMENT", "subcategory":"Settlement Lag","description":"Card/e-wallet settlement received in bank later than expected",       "responsible_team":"Treasury",  "typical_resolution_hrs":24,"is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-010","category":"PAYMENT", "subcategory":"Interchange",   "description":"Interchange fee deducted differs from contracted rate",               "responsible_team":"Treasury",  "typical_resolution_hrs":48,"is_systemic":"Y","occurrence_count":0},
    {"root_cause_code":"RC-011","category":"PAYMENT", "subcategory":"Chargeback",    "description":"Customer chargeback deducted from settlement batch",                  "responsible_team":"Finance",   "typical_resolution_hrs":72,"is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-012","category":"DATA",    "subcategory":"Data Quality",  "description":"Source data quality issue — null, invalid or out-of-range value",     "responsible_team":"IT-POS",    "typical_resolution_hrs":4, "is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-013","category":"CONFIG",  "subcategory":"Tax Code",      "description":"Wrong tax code applied — SST/GST rate mismatch",                      "responsible_team":"IT-SAP",    "typical_resolution_hrs":4, "is_systemic":"Y","occurrence_count":0},
    {"root_cause_code":"RC-014","category":"PAYMENT", "subcategory":"Missing Credit","description":"Bank statement credit not received — bank cut-off or processing delay","responsible_team":"Treasury",  "typical_resolution_hrs":48,"is_systemic":"N","occurrence_count":0},
    {"root_cause_code":"RC-015","category":"SYSTEM",  "subcategory":"FX Conversion", "description":"Cross-border card FX conversion error (tourist spend in SG)",         "responsible_team":"IT-SAP",    "typical_resolution_hrs":8, "is_systemic":"N","occurrence_count":0},
]
write_csv("dim_root_cause.csv", dim_rc,
          ["root_cause_code","category","subcategory","description",
           "responsible_team","typical_resolution_hrs","is_systemic","occurrence_count"])

print("\nDimension tables complete.")