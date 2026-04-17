import sys
import math
import re
from collections import defaultdict

import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from db import get_engine

# =========================
# CONFIG
# =========================
STATUS_LIVE = "LIVE"
STATUS_LOST = {"LOST", "LEAD LOST"}
STATUS_LEAD_PENDING = {"LEAD", "PENDING"}

ANALYSIS_START_MONTH = pd.Timestamp("2021-01-01")
engine = get_engine()
OUTPUT_SCHEMA = "public"

# =========================
# HELPERS
# =========================
def normalize_text(x):
    if x is None:
        return ""
    return re.sub(r"\s+", " ", str(x).strip())

def pct(n, d):
    return 0 if d in (0, None) or pd.isna(d) else round(n*100 / d,2)

def clean_status(x):
    return normalize_text(x).upper()

def clean_mpan(x):
    if pd.isna(x):
        return None
    s = str(x).strip().replace(",", "")
    if s == "":
        return None
    try:
        if "e" in s.lower():
            s = str(int(float(s)))
    except:
        pass
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]
    return s

def parse_mixed_date(x):
    dt = pd.to_datetime(x, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return pd.NaT
    return dt.tz_localize(None).normalize()

def first_day_of_month(ts):
    return pd.Timestamp(year=ts.year, month=ts.month, day=1)

def add_years_safe(dt, years):
    try:
        years = int(float(years))
        return (pd.Timestamp(dt) + relativedelta(years=years) - pd.Timedelta(days=1)).normalize()
    except:
        return pd.NaT

def month_label(ts):
    return ts.strftime("%b-%y")

# =========================
# LOAD DATA
# =========================
raw = pd.read_sql("select * from public_staging.stg_tracker_main_data_raw", engine)
latest = pd.read_sql("select * from public_staging.stg_tracker_main_data", engine)
all_ids = pd.read_sql("select * from public_staging.stg_all_ids", engine)
current = pd.read_sql("select * from public_staging.stg_all_ids", engine)
hist = pd.read_sql("select * from public_staging.stg_historic_ced", engine)

# =========================
# CLEAN RAW
# =========================
raw = raw.rename(columns={
    "rowid": "rowID",
    "clientid": "clientID",
    "contract_duration": "contract_duration",
    "row_add_date": "row_add_date",
    "ecosave_status": "ecosave_status",
    "supplier_csd": "supplier_csd"
})

raw["mpan"] = raw["mpan"].apply(clean_mpan)
raw["ecosave_status"] = raw["ecosave_status"].apply(clean_status)
raw["row_add_date"] = raw["row_add_date"].apply(parse_mixed_date)
raw["supplier_csd"] = raw["supplier_csd"].apply(parse_mixed_date)

raw = raw.dropna(subset=["mpan", "row_add_date"])

latest["mpan"] = latest["mpan"].apply(clean_mpan)
latest["ecosave_status"] = latest["ecosave_status"].apply(clean_status)

all_ids["mpan"] = all_ids["mpan"].apply(clean_mpan)
current["mpan"] = current["mpan"].apply(clean_mpan)

hist["history_value"] = hist["history_value"].apply(parse_mixed_date)
hist["history_timestamp"] = hist["history_timestamp"].apply(parse_mixed_date)

# =========================
# LOOKUPS
# =========================
mpan_to_meter_ids = defaultdict(set)
mpan_to_client_ids = defaultdict(set)

for r in all_ids.itertuples(index=False):
    mpan_to_meter_ids[r.mpan].add(int(r.meterid))
    if pd.notna(r.clientid):
        mpan_to_client_ids[r.mpan].add(str(r.clientid))

current_by_meter = {}
for r in current.itertuples(index=False):
    if pd.notna(r.ced):
        current_by_meter[int(r.meterid)] = r.ced

hist_by_meter = defaultdict(list)
for r in hist.itertuples(index=False):
    if pd.notna(r.history_timestamp) and pd.notna(r.history_value):
        hist_by_meter[int(r.meterid)].append((r.history_timestamp, r.history_value))

for k in hist_by_meter:
    hist_by_meter[k] = sorted(hist_by_meter[k], key=lambda x: x[0])

# =========================
# LIVE PERIODS
# =========================
live_periods_by_mpan = defaultdict(list)

bad_live_rows=[]
for r in raw[raw["ecosave_status"] == STATUS_LIVE].itertuples(index=False):

    start = r.supplier_csd
    end = add_years_safe(start, r.contract_duration)

    # FIX: stricter validation
    if pd.isna(start) or pd.isna(end) or end <= start:

        bad_live_rows.append({
            "mpan": r.mpan,
            "rowID": r.rowID,
            "supplier_csd": start,
            "contract_duration": r.contract_duration,
            "reason": "invalid live contract period"
        })
        continue

    live_periods_by_mpan[r.mpan].append({
        "live_start": start,
        "live_end": end,
        "row_add_date": r.row_add_date,
        "rowID": r.rowID,
        "clientID": r.clientID,
        "contract_duration": r.contract_duration,
        "supplier_csd": r.supplier_csd
    })

for mpan in live_periods_by_mpan:
    live_periods_by_mpan[mpan] = sorted(
        live_periods_by_mpan[mpan],
        key=lambda x: (x["row_add_date"], x["live_start"], x["live_end"])
    )

# =========================
# OPPORTUNITY CYCLES
# =========================
opportunity_cycles = []

for mpan in raw["mpan"].dropna().unique():

    mpan_rows = raw[raw["mpan"] == mpan].sort_values(["row_add_date"])

    first_live = mpan_rows[mpan_rows["ecosave_status"] == STATUS_LIVE].head(1)
    first_live_date = first_live.iloc[0]["row_add_date"] if not first_live.empty else pd.NaT

    pre_live = mpan_rows.copy()
    if pd.notna(first_live_date):
        pre_live = pre_live[pre_live["row_add_date"] < first_live_date]

    pre_live_resale = pre_live[pre_live["ecosave_status"].isin(STATUS_LOST)]

    if not pre_live_resale.empty:

        latest_pre = pre_live_resale.iloc[-1]
        latest_pre_date = latest_pre["row_add_date"]

        best_ced = pd.NaT
        best_source = None
        best_ts = ""

        has_live = pd.notna(first_live_date)

        if not has_live:
            candidates = [
                (m, current_by_meter.get(m))
                for m in mpan_to_meter_ids.get(mpan, [])
                if current_by_meter.get(m)
            ]
            if candidates:
                best_ced = max(candidates, key=lambda x: x[1])[1]
                best_source = "current_ced_never_live"

        else:
            for m in mpan_to_meter_ids.get(mpan, []):
                hist_vals = hist_by_meter.get(m, [])
                before = [h for h in hist_vals if h[0] < first_live_date]

                if before:
                    ts, ced = max(before, key=lambda x: x[0])
                    if pd.isna(best_ced) or ts > pd.to_datetime(best_ts or "1900-01-01"):
                        best_ced = ced
                        best_source = "historic_ced_pre_live"
                        best_ts = ts.strftime("%Y-%m-%d")

            if pd.isna(best_ced):
                candidates = [
                    (m, current_by_meter.get(m))
                    for m in mpan_to_meter_ids.get(mpan, [])
                    if current_by_meter.get(m)
                ]
                if candidates:
                    best_ced = max(candidates, key=lambda x: x[1])[1]
                    best_source = "current_fallback"

        if pd.notna(best_ced):
            entry_month = first_day_of_month(best_ced - relativedelta(years=1))

            if entry_month >= ANALYSIS_START_MONTH:
                opportunity_cycles.append({
                    "mpan": mpan,
                    "cycle_type": "pre_live_cycle",
                    "cycle_sequence": 1,
                    "opportunity_entry_month": entry_month,
                    "effective_ced": best_ced,
                    "ced_source": best_source,
                    "latest_pre_live_resale_date": latest_pre_date
                })

    live_rows = live_periods_by_mpan.get(mpan, [])

    for i, lr in enumerate(live_rows, start=1):
        entry_month = first_day_of_month(lr["live_end"] - relativedelta(years=1))

        if entry_month >= ANALYSIS_START_MONTH:
            opportunity_cycles.append({
                "mpan": mpan,
                "cycle_type": "live_cycle",
                "cycle_sequence": i + 1,
                "opportunity_entry_month": entry_month,
                "effective_ced": lr["live_end"],
                "ced_source": "live_override",
                "source_row_add_date": lr["row_add_date"]
            })

# =========================
# OUTCOMES
# =========================
outcomes = []

for c in opportunity_cycles:

    mpan = c["mpan"]
    entry = pd.Timestamp(c["opportunity_entry_month"])

    rows = raw[raw["mpan"] == mpan]
    later = rows[rows["row_add_date"] > entry]

    sold = "Y" if not later.empty else "N"

    first_follow = later.iloc[0] if not later.empty else None
    last_follow = later.iloc[-1] if not later.empty else None

    outcomes.append({
        "mpan": mpan,
        "cycle_type": c["cycle_type"],
        "cycle_sequence": c["cycle_sequence"],
        "opportunity_entry_month": month_label(entry),
        "sold": sold,
        "first_followup_rowID": None if first_follow is None else first_follow["rowID"],
        "latest_followup_rowID": None if last_follow is None else last_follow["rowID"],
        "latest_status": None if last_follow is None else last_follow["ecosave_status"]
    })
monthly_breakdown = []

for c in opportunity_cycles:

    m = month_label(c["opportunity_entry_month"])

    related_outcomes = [
        o for o in outcomes
        if o["opportunity_entry_month"] == m
    ]

    total = len(related_outcomes)
    sold = sum(1 for o in related_outcomes if o["sold"] == "Y")

    latest_status_live = sum(
        1 for o in related_outcomes if o["latest_status"] == "LIVE"
    )

    latest_status_lost = sum(
        1 for o in related_outcomes if o["latest_status"] in STATUS_LOST
    )

    latest_status_pending = sum(
        1 for o in related_outcomes if o["latest_status"] in STATUS_LEAD_PENDING
    )

    monthly_breakdown.append({
        "month": m,
        "total_opportunities": total,
        "sold": sold,
        "conversion_rate": pct(sold, total),
        "live": latest_status_live,
        "lost": latest_status_lost,
        "pending": latest_status_pending
    })

monthly_breakdown_df = pd.DataFrame(monthly_breakdown)
# =========================
# ANALYSIS
# =========================
analysis = []

months = sorted(set(month_label(c["opportunity_entry_month"]) for c in opportunity_cycles))

for m in months:

    cycles = [c for c in opportunity_cycles if month_label(c["opportunity_entry_month"]) == m]
    outs = [o for o in outcomes if o["opportunity_entry_month"] == m]

    total = len(cycles)
    sold = sum(1 for o in outs if o["sold"] == "Y")

    analysis.append({
        "month": m,
        "opportunities": total,
        "sold": sold,
        "conversion_rate": pct(sold, total)
    })

analysis_df = pd.DataFrame(analysis)
cycles_df = pd.DataFrame(opportunity_cycles)

outcomes_df = pd.DataFrame(outcomes)
monthly_breakdown_df = pd.DataFrame(monthly_breakdown)
monthly_breakdown_df=monthly_breakdown_df.drop_duplicates(subset=["month"])  # (optional placeholder logic if needed)
bad_live_df = pd.DataFrame(bad_live_rows)

# =========================
# WRITE TO POSTGRES (5 OUTPUTS)
# =========================
analysis_df.to_sql("analysis_output", engine, schema=OUTPUT_SCHEMA, if_exists="replace", index=False)
cycles_df.to_sql("opportunity_cycles", engine, schema=OUTPUT_SCHEMA, if_exists="replace", index=False)
outcomes_df.to_sql("opportunity_outcomes", engine, schema=OUTPUT_SCHEMA, if_exists="replace", index=False)
monthly_breakdown_df.to_sql("monthly_breakdown", engine, schema=OUTPUT_SCHEMA, if_exists="replace", index=False)
bad_live_df.to_sql("bad_live_rows", engine, schema=OUTPUT_SCHEMA, if_exists="replace", index=False)

print("✅ DONE - 5 OUTPUT TABLES CREATED")