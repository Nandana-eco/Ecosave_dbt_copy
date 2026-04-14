import pandas as pd
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from sqlalchemy import text
from db import get_engine

# =========================
# CONFIG
# =========================
ANALYSIS_START_MONTH = pd.Timestamp("2021-01-01")

STATUS_LIVE = "LIVE"
STATUS_LOST = {"LOST", "LEAD LOST"}
STATUS_LEAD_PENDING = {"LEAD", "PENDING"}

engine = get_engine()


# =========================
# LOAD DBT MODELS
# =========================
raw = pd.read_sql("select * from public_staging.stg_tracker_main_data_raw", engine)
latest = pd.read_sql("select * from public_staging.stg_tracker_main_data", engine)
all_ids = pd.read_sql("select * from public_staging.stg_all_ids", engine)
current = pd.read_sql("select * from public_staging.stg_all_ids", engine)
hist = pd.read_sql("select * from public_staging.stg_historic_ced", engine)


# =========================
# CLEAN
# =========================
raw["row_add_date"] = pd.to_datetime(raw["row_add_date"],utc=True).dt.tz_convert(None)
raw["supplier_csd"] = pd.to_datetime(raw["supplier_csd"],utc=True).dt.tz_convert(None)
raw["mpan"] = raw["mpan"].astype(str)
raw["ecosave_status"] = raw["ecosave_status"].str.strip().str.upper()
current["ced"] = pd.to_datetime(current["ced"], errors="coerce",utc=True).dt.tz_convert(None)
hist["history_value"] = pd.to_datetime(hist["history_value"], errors="coerce")
hist["history_timestamp"] = pd.to_datetime(hist["history_timestamp"], errors="coerce",utc=True).dt.tz_convert(None)


# =========================
# INDEXES
# =========================
mpan_to_meters = all_ids.groupby("mpan")["meterid"].apply(list).to_dict()

current_by_meter = current.dropna(subset=["ced"]).set_index("meterid")["ced"].to_dict()

hist_by_meter = defaultdict(list)
for r in hist.dropna().itertuples():
    hist_by_meter[r.meterid].append((r.history_timestamp, r.history_value))

for k in hist_by_meter:
    hist_by_meter[k].sort(key=lambda x: x[0])


# =========================
# OUTPUT STORAGE
# =========================
cycles = []
outcomes = []
monthly = []


def month_label(ts):
    return ts.strftime("%b-%y")


def first_month(ts):
    return pd.Timestamp(ts.year, ts.month, 1)


def add_years(dt, years):
    return dt + relativedelta(years=int(years)) - pd.Timedelta(days=1)


# =========================
# MAIN LOOP
# =========================
for mpan in raw["mpan"].dropna().unique():

    df = raw[raw["mpan"] == mpan].sort_values("row_add_date")
    meter_ids = mpan_to_meters.get(mpan, [])

    first_live = df[df["ecosave_status"] == STATUS_LIVE]
    first_live_date = first_live["row_add_date"].min() if not first_live.empty else pd.NaT

    pre_live = df[df["row_add_date"] < first_live_date] if pd.notna(first_live_date) else df
    pre_live_lost = pre_live[pre_live["ecosave_status"].isin(STATUS_LOST)]

    # =========================
    # PRE LIVE CYCLE
    # =========================
    if not pre_live_lost.empty:

        latest_loss = pre_live_lost["row_add_date"].max()
        best_ced = pd.NaT

        if pd.notna(first_live_date):
            for m in meter_ids:
                for ts, ced in hist_by_meter.get(m, []):
                    if ts < first_live_date:
                        best_ced = ced

        if pd.isna(best_ced):
            for m in meter_ids:
                if m in current_by_meter:
                    best_ced = current_by_meter[m]

        if pd.notna(best_ced):
            entry_month = first_month(best_ced - relativedelta(years=1))

            cycle_id = len(cycles) + 1

            cycles.append({
                "cycle_id": cycle_id,
                "mpan": mpan,
                "cycle_type": "pre_live",
                "entry_month": entry_month,
                "ced": best_ced
            })

            outcomes.append({
                "cycle_id": cycle_id,
                "mpan": mpan,
                "sold": len(pre_live_lost) > 0,
                "status": "pre_live_generated"
            })

    # =========================
    # LIVE CYCLES
    # =========================
    for _, r in df[df["ecosave_status"] == STATUS_LIVE].iterrows():

        if pd.notna(r["supplier_csd"]) and pd.notna(r["contract_duration"]):

            ced = add_years(r["supplier_csd"], r["contract_duration"])
            entry_month = first_month(ced - relativedelta(years=1))

            cycle_id = len(cycles) + 1

            cycles.append({
                "cycle_id": cycle_id,
                "mpan": mpan,
                "cycle_type": "live",
                "entry_month": entry_month,
                "ced": ced
            })

            outcomes.append({
                "cycle_id": cycle_id,
                "mpan": mpan,
                "sold": True,
                "status": "live_generated"
            })


# =========================
# DATAFRAMES
# =========================
df_cycles = pd.DataFrame(cycles)
df_outcomes = pd.DataFrame(outcomes)


# =========================
# WRITE TO POSTGRES (IMPORTANT PART)
# =========================
def write_table(df, table_name):
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))
        df.to_sql(table_name, conn, index=False, if_exists="append")


write_table(df_cycles, "fct_opportunity_cycles")
write_table(df_outcomes, "fct_opportunity_outcomes")


print("DONE ✔")
print("Written to Postgres:")
print("- fct_opportunity_cycles")
print("- fct_opportunity_outcomes")