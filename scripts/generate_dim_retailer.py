import random
import os
import pandas as pd
from pathlib import Path
from datetime import date, timedelta

random.seed(42)

# ── PATHS ──────────────────────────────────────────────────────────────────────
BASE = Path(r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data")
DIM_TERRITORY   = BASE / "dim_territory.csv"
DIM_DISTRIBUTOR = BASE / "dim_distributor.csv"
DIM_SKU         = BASE / "dim_sku.csv"
OUTPUT_FILE     = BASE / "dim_retailer.csv"

# ── LOAD & VALIDATE REFERENCE FILES ───────────────────────────────────────────
df_territory   = pd.read_csv(DIM_TERRITORY)
df_distributor = pd.read_csv(DIM_DISTRIBUTOR)
df_sku         = pd.read_csv(DIM_SKU)  # read for referential completeness

valid_territory_ids   = set(df_territory["territory_id"].astype(str))
valid_distributor_ids = set(df_distributor["distributor_id"].astype(str))

# ── TERRITORY CONFIG ───────────────────────────────────────────────────────────
# (territory_id, town_name, [pincodes], {fy: (lo, hi) or fixed_int}, [dist_ids])
TERRITORY_CONFIG = [
    # ── COHORT 1 ──────────────────────────────────────────────────────────────
    ("T-WB-01","Serampore",  [712201,712202,712203],
        {"Y1":100,"Y2":(28,32),"Y3":(23,27)}, ["DIST-001"]),
    ("T-WB-02","Chinsurah",  [712101,712102,712104],
        {"Y1":100,"Y2":(28,32),"Y3":(23,27)}, ["DIST-002"]),
    ("T-WB-03","Arambagh",   [712601,712602],
        {"Y1":95, "Y2":(26,30),"Y3":(21,25)}, ["DIST-003"]),
    ("T-WB-04","Bardhaman",  [713101,713102,713103,713104],
        {"Y1":100,"Y2":(28,32),"Y3":(23,27)}, ["DIST-004","DIST-005"]),
    ("T-WB-05","Katwa",      [713130,713131],
        {"Y1":95, "Y2":(26,30),"Y3":(21,25)}, ["DIST-006"]),
    ("T-WB-06","Uluberia",   [711315,711316,711317],
        {"Y1":95, "Y2":(26,30),"Y3":(21,25)}, ["DIST-007"]),
    ("T-WB-07","Asansol",    [713301,713302,713303],
        {"Y1":100,"Y2":(28,32),"Y3":(23,27)}, ["DIST-008","DIST-009"]),
    ("T-WB-08","Asansol",    [713304,713305,713321],
        {"Y1":120,"Y2":20,     "Y3":0},        ["DIST-010"]),   # SPECIAL
    ("T-WB-09","Durgapur",   [713201,713202,713203,713204],
        {"Y1":95, "Y2":(26,30),"Y3":(21,25)}, ["DIST-011"]),
    # ── COHORT 2 ──────────────────────────────────────────────────────────────
    ("T-WB-10","Kharagpur",  [721301,721302,721305],
        {"Y2":(68,72),"Y3":(23,27)},           ["DIST-012","DIST-013"]),
    ("T-WB-11","Medinipur",  [721101,721102],
        {"Y2":(63,67),"Y3":(21,25)},           ["DIST-014"]),
    ("T-WB-12","Bankura",    [722101,722102],
        {"Y2":(63,67),"Y3":(21,25)},           ["DIST-015"]),
    ("T-WB-13","Bishnupur",  [722122,722123],
        {"Y2":(58,62),"Y3":(19,23)},           ["DIST-016"]),
    ("T-WB-14","Kalyani",    [741235,741249,741251],
        {"Y2":(68,72),"Y3":(23,27)},           ["DIST-017"]),
    ("T-WB-15","Krishnanagar",[741101,741102,741103],
        {"Y2":(65,70),"Y3":(21,25)},           ["DIST-018"]),
    ("T-WB-16","Baharampur", [742101,742102,742103],
        {"Y2":(65,70),"Y3":(21,25)},           ["DIST-019"]),
    ("T-OD-01","Balasore",   [756001,756002,756003],
        {"Y2":(53,57),"Y3":(23,27)},           ["DIST-020"]),
    ("T-OD-02","Jaleswar",   [756032,756033],
        {"Y2":(43,47),"Y3":(21,25)},           ["DIST-021"]),
    ("T-OD-03","Bhadrak",    [756100,756101,756113],
        {"Y2":(48,52),"Y3":(21,25)},           ["DIST-022"]),
    # ── COHORT 3 ──────────────────────────────────────────────────────────────
    ("T-WB-17","Jangipur",   [742213,742214],
        {"Y3":(63,67)},                         ["DIST-023"]),
    ("T-WB-18","Siliguri",   [734001,734003,734004,734005],
        {"Y3":(73,77)},                         ["DIST-024"]),
    ("T-WB-19","Malda",      [732101,732102,732103],
        {"Y3":(66,70)},                         ["DIST-025"]),
    ("T-OD-04","Cuttack",    [753001,753002,753003,753004],
        {"Y3":(68,72)},                         ["DIST-026","DIST-027"]),
    ("T-OD-05","Bhubaneswar",[751001,751002,751003,751007],
        {"Y3":(70,74)},                         ["DIST-028","DIST-029"]),
    ("T-OD-06","Khordha",    [752055,752056],
        {"Y3":(46,50)},                         ["DIST-030"]),
    ("T-AS-01","Guwahati",   [781001,781003,781006,781009],
        {"Y3":(43,47)},                         ["DIST-031","DIST-032"]),
    ("T-AS-02","Guwahati",   [781005,781007,781010,781014],
        {"Y3":(43,47)},                         ["DIST-033","DIST-034"]),
    ("T-AS-03","Nagaon",     [782001,782002,782003],
        {"Y3":(40,44)},                         ["DIST-035"]),
    ("T-AS-04","Tezpur",     [784001,784002],
        {"Y3":(38,42)},                         ["DIST-036"]),
]

# Validate reference IDs from config
for ter_id, _, _, _, dist_ids in TERRITORY_CONFIG:
    if ter_id not in valid_territory_ids:
        raise ValueError(f"Territory {ter_id} not found in dim_territory.csv")
    for d in dist_ids:
        if d not in valid_distributor_ids:
            raise ValueError(f"Distributor {d} not found in dim_distributor.csv")

# ── FY DATE RANGES ─────────────────────────────────────────────────────────────
FY_RANGES = {
    "Y1": (date(2023, 4, 15), date(2024, 3, 31)),  # actual launch date
    "Y2": (date(2024, 4,  1), date(2025, 3, 31)),
    "Y3": (date(2025, 4,  1), date(2026, 3, 31)),
}
FY_START = {
    "Y1": date(2023, 4, 1),   # logical FY start for month-bucketing
    "Y2": date(2024, 4, 1),
    "Y3": date(2025, 4, 1),
}

# ── RAMP-UP WEIGHTS per quarter (months 1-3, 4-6, 7-9, 10-12) ─────────────────
RAMP = [0.10, 0.20, 0.35, 0.35]   # fraction of year's stores per quarter

def get_month_offset(fy_key, month_1based):
    """Return (year, month) for month_1based (1..12) within a FY."""
    base = FY_START[fy_key]
    m = base.month + month_1based - 1
    y = base.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    return y, m

def working_days_in_month(year, month):
    """Return list of Mon-Sat dates in the given month."""
    import calendar
    _, last = calendar.monthrange(year, month)
    days = []
    for d in range(1, last + 1):
        dt = date(year, month, d)
        if dt.weekday() < 6:  # Mon=0 … Sat=5
            days.append(dt)
    return days

def distribute_dates(n_stores, fy_key, start_override=None, end_override=None):
    """
    Distribute n_stores onboard dates across 12 months of a FY
    using the ramp-up curve. Returns a sorted list of date objects.
    """
    if n_stores == 0:
        return []
    # Allocate stores to quarters
    quarters = [0, 0, 0, 0]
    remaining = n_stores
    for i in range(3):
        quarters[i] = round(RAMP[i] * n_stores)
        remaining -= quarters[i]
    quarters[3] = remaining  # absorb rounding

    dates = []
    for q_idx, count in enumerate(quarters):
        if count == 0:
            continue
        months_in_q = range(q_idx * 3 + 1, q_idx * 3 + 4)  # 1-based months
        # spread count evenly across 3 months in quarter
        per_month = [0, 0, 0]
        rem = count
        for i in range(2):
            per_month[i] = count // 3
            rem -= per_month[i]
        per_month[2] = rem

        for m_offset, pm_count in zip(months_in_q, per_month):
            if pm_count == 0:
                continue
            yr, mo = get_month_offset(fy_key, m_offset)
            wdays = working_days_in_month(yr, mo)
            # filter by override bounds
            lo = start_override or FY_RANGES[fy_key][0]
            hi = end_override or FY_RANGES[fy_key][1]
            wdays = [d for d in wdays if lo <= d <= hi]
            if not wdays:
                # fallback: just use hi
                wdays = [hi]
            chosen = [random.choice(wdays) for _ in range(pm_count)]
            dates.extend(chosen)
    return sorted(dates)

# ── NAME GENERATION ────────────────────────────────────────────────────────────
WB_SURNAMES  = ["Chatterjee","Mukherjee","Banerjee","Das","Ghosh","Roy","Sen",
                 "Saha","Paul","Dey","Bose","Mondal","Pal","Majumdar","Biswas",
                 "Mitra","Nandi","Kundu","Sarkar","Chakraborty","Haldar","Patra"]
WB_DEITIES   = ["Durga","Kali","Lakshmi","Saraswati","Shiva","Vishnu","Ganesh","Radha"]
WB_PLACES    = ["Nabagram","Purba","Paschim","Uttar","Dakhin"]

OD_SURNAMES  = ["Panda","Nayak","Behera","Mohapatra","Sahu","Jena","Mishra",
                 "Das","Swain","Biswal","Rout","Pradhan","Senapati","Parida",
                 "Barik","Sahoo","Dash","Mallick"]
OD_DEITIES   = ["Durga","Lakshmi","Mangala","Tarini","Jagannath"]

AS_SURNAMES  = ["Kalita","Borah","Hazarika","Choudhury","Baruah","Sharma","Das",
                 "Deka","Nath","Saikia","Bordoloi","Talukdar","Medhi","Gogoi",
                 "Konwar","Phukan","Mahanta"]
AS_WORDS     = ["Udyog","Bazar","Traders"]

def _wb_name():
    fmt = random.choice([
        "Maa {g} Bhandar",
        "{s} & Sons",
        "{s} General Store",
        "Jai {g} Stores",
        "{p} Kirana Bhandar",
        "{s} Brothers",
        "Shree {g} Traders",
        "{s} Provision Store",
    ])
    return fmt.format(s=random.choice(WB_SURNAMES),
                      g=random.choice(WB_DEITIES),
                      p=random.choice(WB_PLACES))

def _od_name():
    fmt = random.choice([
        "Maa {g} Bhandar",
        "{s} & Sons",
        "{s} General Store",
        "Jai Jagannath Traders",
        "{s} Provision Store",
        "Shree Balaji Stores",
        "{s} Brothers",
    ])
    return fmt.format(s=random.choice(OD_SURNAMES),
                      g=random.choice(OD_DEITIES))

def _as_name():
    fmt = random.choice([
        "{s} Store",
        "{s} & Brothers",
        "{s} Traders",
        "Maa {g} Bhandar",  # <--- UPDATED PLACEHOLDER
        "{s} General Store",
        "Axom {w} Store",
        "{s} Provision Store",
    ])
    return fmt.format(s=random.choice(AS_SURNAMES),
                      w=random.choice(AS_WORDS),
                      g=random.choice(WB_DEITIES)) # <--- ADDED DEITY POOL


def gen_name(ter_id):
    if ter_id.startswith("T-WB"):
        return _wb_name()
    elif ter_id.startswith("T-OD"):
        return _od_name()
    else:
        return _as_name()

# ── MAIN ROW GENERATION ────────────────────────────────────────────────────────
rows = []          # list of dicts (no retailer_id yet)
used_names = set()

def unique_name(ter_id):
    base = gen_name(ter_id)
    if base not in used_names:
        used_names.add(base)
        return base
    # try a few variants
    for i in range(2, 100):
        candidate = f"{base} - {i:02d}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
    raise RuntimeError("Could not generate unique name")

def split_stores(total, n_dists):
    """Split total stores across n_dists distributors, ±5 stores."""
    if n_dists == 1:
        return [total]
    base = total // n_dists
    splits = []
    allocated = 0
    for i in range(n_dists - 1):
        # allow ±5 variation
        lo = max(1, base - 5)
        hi = base + 5
        s = random.randint(lo, hi)
        splits.append(s)
        allocated += s
    splits.append(total - allocated)
    return splits

# ── Collect rows grouped by territory ─────────────────────────────────────────
# We need to track per-territory rows for churn assignment later
territory_rows = {cfg[0]: [] for cfg in TERRITORY_CONFIG}

for ter_id, town_name, pincodes, counts, dist_ids in TERRITORY_CONFIG:
    n_dists = len(dist_ids)

    for fy_key in ["Y1", "Y2", "Y3"]:
        if fy_key not in counts:
            continue
        spec = counts[fy_key]
        if isinstance(spec, tuple):
            n_stores = random.randint(*spec)
        else:
            n_stores = spec  # fixed integer
        if n_stores == 0:
            continue

        # SPECIAL CASE T-WB-08 Y2: dates restricted to Apr-Sep 2024
        if ter_id == "T-WB-08" and fy_key == "Y2":
            dates = []
            for _ in range(n_stores):
                # Pick a random date between Apr 1 and Sep 30
                delta = (date(2024, 9, 30) - date(2024, 4, 1)).days
                while True:
                    dt = date(2024, 4, 1) + timedelta(days=random.randint(0, delta))
                    if dt.weekday() < 6:
                        dates.append(dt)
                        break
            dates.sort()
        else:
            dates = distribute_dates(n_stores, fy_key)

        # split across distributors
        splits = split_stores(n_stores, n_dists)
        all_dates = list(dates)
        random.shuffle(all_dates)
        offset = 0
        for dist_id, d_count in zip(dist_ids, splits):
            d_count = min(d_count, len(all_dates) - offset)
            for i in range(d_count):
                dt = all_dates[offset + i]
                row = {
                    "retailer_name":    unique_name(ter_id),
                    "distributor_id":   dist_id,
                    "territory_id":     ter_id,
                    "outlet_type":      random.choices(
                                            ["Kirana Store","General Store"],
                                            weights=[90, 10])[0],
                    "town_name":        town_name,
                    "pincode":          str(random.choice(pincodes)),
                    "onboard_date":     dt,
                    "status":           "Active",
                    "deactivation_date": "",
                    "_fy":              fy_key,
                }
                rows.append(row)
                territory_rows[ter_id].append(row)
            offset += d_count

# ── CHURN WAVE 1 (FY2024-25, 5% of Y1 = 45 stores) ───────────────────────────
FY2_START = date(2024, 4, 1)
FY2_END   = date(2025, 3, 31)

# Pre-define allocation
churn_w1_alloc = {
    "T-WB-08": 8,
    "T-WB-03": 5,
    "T-WB-05": 5,
    "T-WB-06": 5,
}
# remaining 22 across T-WB-01, T-WB-02, T-WB-04, T-WB-07, T-WB-09
remaining_22_ters = ["T-WB-01","T-WB-02","T-WB-04","T-WB-07","T-WB-09"]

def get_y1_active(ter_id):
    return [r for r in territory_rows[ter_id]
            if r["_fy"] == "Y1" and r["status"] == "Active"]

# Allocate remaining 22
rem_pool = []
for t in remaining_22_ters:
    rem_pool.extend(get_y1_active(t))
random.shuffle(rem_pool)
rem_22 = rem_pool[:22]
for r in rem_22:
    churn_w1_alloc[r["territory_id"]] = churn_w1_alloc.get(r["territory_id"], 0) + 1

# Apply Wave 1 churn
def apply_churn(candidates, n, date_lo, date_hi):
    chosen = random.sample(candidates, min(n, len(candidates)))
    for r in chosen:
        lo = max(r["onboard_date"] + timedelta(days=1), date_lo)
        hi = date_hi
        if lo > hi:
            lo = r["onboard_date"] + timedelta(days=1)
        delta = (hi - lo).days
        if delta < 0:
            delta = 0
        deact = lo + timedelta(days=random.randint(0, max(delta, 0)))
        r["status"] = "Inactive"
        r["deactivation_date"] = deact.strftime("%Y-%m-%d")
    return chosen

# Wave 1 – named territories
for ter_id, count in list(churn_w1_alloc.items()):
    if ter_id in remaining_22_ters:
        continue  # handled separately below
    candidates = get_y1_active(ter_id)
    apply_churn(candidates, count, FY2_START, FY2_END)

# Wave 1 – remaining 22 (mark the ones we already picked)
apply_churn(rem_22, 22, FY2_START, FY2_END)

# ── CHURN WAVE 2 (FY2025-26, ~3% of surviving Y1+Y2) ─────────────────────────
FY3_START = date(2025, 4, 1)
FY3_END   = date(2026, 3, 31)

# Odisha Y2 territories: 15 total
od_y2_ters = ["T-OD-01","T-OD-02","T-OD-03"]
od_y2_pool = []
for t in od_y2_ters:
    od_y2_pool.extend([r for r in territory_rows[t]
                       if r["status"] == "Active"])
random.shuffle(od_y2_pool)
od_15 = od_y2_pool[:15]
apply_churn(od_15, 15, FY3_START, FY3_END)

# T-WB-08: 6 more
wb08_pool = [r for r in territory_rows["T-WB-08"] if r["status"] == "Active"]
apply_churn(wb08_pool, 6, FY3_START, FY3_END)

# Remaining ~31 from Y1+Y2 territories (excluding already-handled ones)
exclude_w2 = set(["T-OD-01","T-OD-02","T-OD-03","T-WB-08"])
w2_general_pool = []
for r in rows:
    if r["territory_id"] not in exclude_w2 and r["status"] == "Active" \
            and r["_fy"] in ("Y1","Y2"):
        w2_general_pool.append(r)
random.shuffle(w2_general_pool)
apply_churn(w2_general_pool, 31, FY3_START, FY3_END)

# ── SORT & ASSIGN RETAILER IDs ─────────────────────────────────────────────────
rows.sort(key=lambda r: (r["onboard_date"], r["territory_id"], r["distributor_id"]))
for idx, row in enumerate(rows, 1):
    row["retailer_id"] = f"RET-{idx:05d}"

# ── FORMAT DATES AS STRINGS ────────────────────────────────────────────────────
for row in rows:
    row["onboard_date"] = row["onboard_date"].strftime("%Y-%m-%d")

# ── BUILD DATAFRAME ────────────────────────────────────────────────────────────
COLUMNS = ["retailer_id","retailer_name","distributor_id","territory_id",
           "outlet_type","town_name","pincode","onboard_date","status",
           "deactivation_date"]
df = pd.DataFrame(rows, columns=COLUMNS + ["_fy"])

# ── VALIDATION ─────────────────────────────────────────────────────────────────
def check(condition, msg):
    if not condition:
        raise AssertionError(f"FAILED: {msg}")
    print(f"  PASSED: {msg}")

print("\n── Running validation checks ──")

# 1. No duplicate retailer_id
check(df["retailer_id"].nunique() == len(df), "No duplicate retailer_id")

# 2. No duplicate retailer_name
check(df["retailer_name"].nunique() == len(df), "No duplicate retailer_name")

# 3. Every distributor_id exists in dim_distributor.csv
check(set(df["distributor_id"]).issubset(valid_distributor_ids),
      "All distributor_ids valid")

# 4. Every territory_id exists in dim_territory.csv
check(set(df["territory_id"]).issubset(valid_territory_ids),
      "All territory_ids valid")

# 5. All FY2023-24 onboard_dates >= 2023-04-15
y1_dates = df[df["_fy"] == "Y1"]["onboard_date"]
check(all(d >= "2023-04-15" for d in y1_dates),
      "All Y1 onboard_dates >= 2023-04-15")

# 6. T-WB-08 counts
wb08 = df[df["territory_id"] == "T-WB-08"]
wb08_y1 = wb08[wb08["_fy"] == "Y1"]
wb08_y2 = wb08[wb08["_fy"] == "Y2"]
wb08_y3 = wb08[wb08["_fy"] == "Y3"]
check(len(wb08_y1) == 120, f"T-WB-08 Y1 = 120 (got {len(wb08_y1)})")
check(len(wb08_y2) == 20,  f"T-WB-08 Y2 = 20 (got {len(wb08_y2)})")
check(len(wb08_y3) == 0,   f"T-WB-08 Y3 = 0 (got {len(wb08_y3)})")

# 7. T-WB-08 Y2 dates within Apr-Sep 2024
wb08_y2_dates = wb08_y2["onboard_date"]
check(all("2024-04-01" <= d <= "2024-09-30" for d in wb08_y2_dates),
      "T-WB-08 Y2 dates all within 2024-04-01 to 2024-09-30")

# 8. Total inactive approx 95-100
total_inactive = (df["status"] == "Inactive").sum()
check(95 <= total_inactive <= 105,
      f"Total inactive between 95-105 (got {total_inactive})")

# 9. Deactivation_dates strictly after onboard_date
inactive_df = df[df["status"] == "Inactive"]
check(all(row["deactivation_date"] > row["onboard_date"]
          for _, row in inactive_df.iterrows()),
      "All deactivation_dates > onboard_date")

# 10. outlet_type values
check(set(df["outlet_type"]).issubset({"Kirana Store","General Store"}),
      "outlet_type values valid")

# ── CONSOLE SUMMARY ────────────────────────────────────────────────────────────
print(f"\n── Summary ──")
print(f"Total rows generated : {len(df)}")
print(f"Active stores        : {(df['status']=='Active').sum()}")
print(f"Inactive stores      : {(df['status']=='Inactive').sum()}")
print("\nStores by territory:")
tcount = df.groupby("territory_id").size().reset_index(name="count")
for _, row in tcount.iterrows():
    print(f"  {row['territory_id']} | {row['count']}")
print("\nStores by FY cohort:")
fy_map = {"Y1":"FY2023-24","Y2":"FY2024-25","Y3":"FY2025-26"}
for fy_key, fy_label in fy_map.items():
    print(f"  {fy_label} | {(df['_fy']==fy_key).sum()}")

# ── WRITE OUTPUT ───────────────────────────────────────────────────────────────
df_out = df[COLUMNS]
df_out.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
print(f"\nSaved → {OUTPUT_FILE}")

