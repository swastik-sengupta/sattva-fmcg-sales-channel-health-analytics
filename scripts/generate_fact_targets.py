import os
import random
import pandas as pd
from pathlib import Path
from datetime import date

random.seed(42)

# ── File Paths ──────────────────────────────────────────────────────────────
BASE = Path(r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data")

dim_territory = pd.read_csv(BASE / "dim_territory.csv")
dim_retailer  = pd.read_csv(BASE / "dim_retailer.csv", parse_dates=["onboard_date"])

# Parse deactivation_date: blank → NaT
dim_retailer["deactivation_date"] = pd.to_datetime(
    dim_retailer["deactivation_date"].replace("", pd.NaT), errors="coerce"
)

valid_territory_ids = set(dim_territory["territory_id"].astype(str).str.strip())

# ── Cohort Definitions ───────────────────────────────────────────────────────
COHORT_1 = [
    "T-WB-01","T-WB-02","T-WB-03","T-WB-04","T-WB-05",
    "T-WB-06","T-WB-07","T-WB-08","T-WB-09",
]
COHORT_2 = [
    "T-WB-10","T-WB-11","T-WB-12","T-WB-13","T-WB-14",
    "T-WB-15","T-WB-16","T-OD-01","T-OD-02","T-OD-03",
]
COHORT_3 = [
    "T-WB-17","T-WB-18","T-WB-19","T-OD-04","T-OD-05",
    "T-OD-06","T-AS-01","T-AS-02","T-AS-03","T-AS-04",
]

# Validate all territory IDs exist
all_used = set(COHORT_1 + COHORT_2 + COHORT_3)
missing = all_used - valid_territory_ids
if missing:
    raise ValueError(f"Territory IDs not found in dim_territory.csv: {missing}")

# ── Fiscal Year / Quarter Helpers ────────────────────────────────────────────
def fiscal_year(dt):
    y, m = dt.year, dt.month
    if m >= 4:
        return f"FY{y}-{str(y+1)[-2:]}"
    else:
        return f"FY{y-1}-{str(y)[-2:]}"

def fiscal_quarter(dt):
    m = dt.month
    if m in (4, 5, 6):   return "Q1"
    if m in (7, 8, 9):   return "Q2"
    if m in (10,11,12):  return "Q3"
    return "Q4"

def month_name(dt):
    return dt.strftime("%b-%Y")

# ── Month sequences ──────────────────────────────────────────────────────────
def month_range(start_year, start_month, n):
    months = []
    y, m = start_year, start_month
    for _ in range(n):
        months.append(date(y, m, 1))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months

FY2324 = month_range(2023, 4, 12)  # Apr 2023 – Mar 2024
FY2425 = month_range(2024, 4, 12)  # Apr 2024 – Mar 2025
FY2526 = month_range(2025, 4, 12)  # Apr 2025 – Mar 2026

# ── Per-store monthly revenue target by region ───────────────────────────────
PER_STORE_RATE = {
    ("WB", "FY2023-24"): 3_180,
    ("WB", "FY2024-25"): 1_930,
    ("WB", "FY2025-26"): 2_340,
    ("OD", "FY2024-25"): 1_700,
    ("OD", "FY2025-26"): 2_050,
    ("AS", "FY2025-26"): 1_950,
}

def per_store_rate(tid, fy):
    if   tid.startswith("T-WB-"): region = "WB"
    elif tid.startswith("T-OD-"): region = "OD"
    elif tid.startswith("T-AS-"): region = "AS"
    else: raise ValueError(f"Unknown region for territory {tid}")
    return PER_STORE_RATE[(region, fy)]

# ── Expected active stores from dim_retailer ─────────────────────────────────
def expected_active_stores(tid, month_start):
    ts = pd.Timestamp(month_start)
    mask = (
        (dim_retailer["territory_id"] == tid) &
        (dim_retailer["onboard_date"] <= ts) &
        (dim_retailer["deactivation_date"].isna() | (dim_retailer["deactivation_date"] > ts))
    )
    return int(mask.sum())

# ── New store target lookup ───────────────────────────────────────────────────

# Quarter index within FY (0-based): Q1=0,Q2=1,Q3=2,Q4=3
def fy_quarter_idx(dt):
    return {"Q1":0,"Q2":1,"Q3":2,"Q4":3}[fiscal_quarter(dt)]

def new_store_target(tid, dt, fy):
    q = fiscal_quarter(dt)

    # ── T-WB-08 special case ──
    if tid == "T-WB-08":
        if fy == "FY2023-24":
            return {0:5,1:8,2:11,3:11}[fy_quarter_idx(dt)]
        if fy == "FY2024-25":
            if dt.month in (4,5,6,7,8,9): return 3
            return 2
        if fy == "FY2025-26":
            return 0

    # ── Cohort 1 (non-WB-08) ──
    if tid in COHORT_1:
        if fy == "FY2023-24":
            return {0:5,1:8,2:11,3:11}[fy_quarter_idx(dt)]
        if fy == "FY2024-25":
            return 3
        if fy == "FY2025-26":
            return 2

    # ── Cohort 2 first year (FY2024-25) ──
    if tid in COHORT_2 and fy == "FY2024-25":
        qi = fy_quarter_idx(dt)
        if tid == "T-WB-10":                              return [6,9,12,12][qi]
        if tid == "T-WB-14":                              return [6,9,12,12][qi]
        if tid in ("T-WB-11","T-WB-12","T-WB-15","T-WB-16"): return [5,8,11,11][qi]
        if tid == "T-WB-13":                              return [4,7,10,10][qi]
        if tid == "T-OD-01":                              return [4,7,10,10][qi]
        if tid == "T-OD-02":                              return [3,5,8,8][qi]
        if tid == "T-OD-03":                              return [3,6,9,9][qi]

    # ── Cohort 2 second year (FY2025-26) ──
    if tid in COHORT_2 and fy == "FY2025-26":
        if tid.startswith("T-WB-"): return 3
        return 2  # T-OD-01/02/03

    # ── Cohort 3 first (and only) year (FY2025-26) ──
    if tid in COHORT_3 and fy == "FY2025-26":
        qi = fy_quarter_idx(dt)
        if tid == "T-WB-18":                              return [6,9,12,12][qi]
        if tid in ("T-WB-17","T-WB-19"):                  return [5,8,11,11][qi]
        if tid in ("T-OD-04","T-OD-05"):                  return [5,8,11,11][qi]
        if tid == "T-OD-06":                              return [3,5,8,8][qi]
        if tid in ("T-AS-01","T-AS-02"):                  return [3,5,8,8][qi]
        if tid in ("T-AS-03","T-AS-04"):                  return [3,4,7,7][qi]

    raise ValueError(f"No new_store_target rule for {tid} / {fy}")

# ── Revenue target calculation ────────────────────────────────────────────────
def revenue_target(tid, dt, fy, nst):
    rate = per_store_rate(tid, fy)
    active = expected_active_stores(tid, dt)
    if active == 0:
        raw = nst * rate * 0.5
    else:
        raw = active * rate

    # Apply the discount to the RAW value first
    if tid == "T-WB-08" and fy in ("FY2024-25", "FY2025-26"):
        raw = raw * 0.80

    # Do the rounding once at the very end. 
    # Note: round(raw, -3) is Python's cleaner built-in way to round to the nearest 1000!
    return int(round(raw, -3))


# ── Build rows ────────────────────────────────────────────────────────────────
rows = []

territory_fy_map = (
    [(t, FY2324 + FY2425 + FY2526) for t in COHORT_1] +
    [(t, FY2425 + FY2526)          for t in COHORT_2] +
    [(t, FY2526)                   for t in COHORT_3]
)

for tid, months in territory_fy_map:
    for dt in months:
        fy = fiscal_year(dt)
        nst = new_store_target(tid, dt, fy)
        rev = revenue_target(tid, dt, fy, nst)
        rows.append({
            "territory_id":       tid,
            "month_start_date":   dt,
            "month_name":         month_name(dt),
            "fiscal_year":        fy,
            "fiscal_quarter":     fiscal_quarter(dt),
            "new_store_target":   nst,
            "revenue_target_inr": rev,
        })

df = pd.DataFrame(rows)

# ── Sort and assign target_id ─────────────────────────────────────────────────
df = df.sort_values(["month_start_date","territory_id"]).reset_index(drop=True)
df.insert(0, "target_id", [f"TGT-{i+1:05d}" for i in range(len(df))])

# Convert date to string
df["month_start_date"] = df["month_start_date"].apply(lambda d: d.strftime("%Y-%m-%d"))

# Ensure column order
df = df[["target_id","territory_id","month_start_date","month_name",
         "fiscal_year","fiscal_quarter","new_store_target","revenue_target_inr"]]

# ── Validation Assertions ─────────────────────────────────────────────────────
def check(condition, msg):
    if not condition:
        raise AssertionError(msg)
    print(f"  PASSED: {msg}")

print("\nRunning validation assertions...")

check(len(df) == 684,
      "Total row count is exactly 684")

check(df["target_id"].nunique() == len(df),
      "No target_id is duplicated")

check(set(df["territory_id"]).issubset(valid_territory_ids),
      "Every territory_id exists in dim_territory.csv")

check(df["month_start_date"].apply(lambda s: s.endswith("-01")).all(),
      "All month_start_date values are the 1st of their month")

check((df["territory_id"] == "T-WB-08").sum() == 36,
      "T-WB-08 has exactly 36 rows")

wb08_fy26 = df[(df["territory_id"]=="T-WB-08") & (df["fiscal_year"]=="FY2025-26")]
check((wb08_fy26["new_store_target"] == 0).all(),
      "T-WB-08 FY2025-26 rows all have new_store_target == 0")

for tid in COHORT_3:
    cnt  = (df["territory_id"]==tid).sum()
    fys  = df.loc[df["territory_id"]==tid, "fiscal_year"].unique().tolist()
    check(cnt == 12 and fys == ["FY2025-26"],
          f"Cohort 3 territory {tid} has exactly 12 rows all in FY2025-26")

# Edge case: first month with active=0 AND nst=0 → revenue=0; should be none
# Normal first-month rows where nst>0 will have revenue>0 via proxy formula
bad_rev = df[(df["revenue_target_inr"] <= 0) & (df["new_store_target"] != 0)]
check(len(bad_rev) == 0,
      "No revenue_target_inr value is zero or negative")

check(set(df["fiscal_quarter"]) == {"Q1","Q2","Q3","Q4"},
      "All fiscal_quarter values are one of Q1/Q2/Q3/Q4")

check(set(df["fiscal_year"]) == {"FY2023-24","FY2024-25","FY2025-26"},
      "All fiscal_year values are one of the three expected FYs")

# ── Console Summary ────────────────────────────────────────────────────────────
total_rev = df["revenue_target_inr"].sum()
def fmt_inr(n):
    # Indian comma formatting
    s = str(n)
    if len(s) <= 3:
        return "₹" + s
    last3 = s[-3:]
    rest  = s[:-3]
    parts = []
    while len(rest) > 2:
        parts.append(rest[-2:])
        rest = rest[:-2]
    if rest:
        parts.append(rest)
    return "₹" + ",".join(reversed(parts)) + "," + last3

print("\nSummary:")
print(f"  Total rows generated: {len(df)}")
print("  Rows by FY:")
for fy in ["FY2023-24","FY2024-25","FY2025-26"]:
    print(f"    {fy}: {(df['fiscal_year']==fy).sum()} rows")
print(f"  Territories covered: {df['territory_id'].nunique()}")
print(f"  Total new_store_target (all rows): {df['new_store_target'].sum():,}")
print(f"  Total revenue_target_inr (all rows): {fmt_inr(total_rev)}")
print("\n  FY-wise Revenue Targets vs Expected Achievement:")
sec_actuals = {"FY2023-24": 8_910_878, "FY2024-25": 21_998_226, "FY2025-26": 47_171_268}
for fy_lbl in ["FY2023-24", "FY2024-25", "FY2025-26"]:
    fy_rev = int(df[df["fiscal_year"] == fy_lbl]["revenue_target_inr"].sum())
    actual = sec_actuals.get(fy_lbl, 0)
    pct    = actual / fy_rev * 100 if fy_rev > 0 else 0
    print(f"    {fy_lbl}: Target {fmt_inr(fy_rev)} | Actual ₹{actual:,} | Achievement: {pct:.1f}%")

# ── Write CSV ──────────────────────────────────────────────────────────────────
out_path = BASE / "fact_target.csv"
df.to_csv(out_path, index=False, encoding="utf-8")
print(f"\nSaved: {out_path}")

