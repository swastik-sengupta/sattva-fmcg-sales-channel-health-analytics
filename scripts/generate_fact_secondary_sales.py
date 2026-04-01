#!/usr/bin/env python3
"""
generate_fact_secondary_sales.py
Sattva Foods Pvt. Ltd. — Synthetic Secondary Sales Data Generator
Target : 380,000 – 420,000 rows | 3 Financial Years | 36 Distributors
Output : fact_secondary_sales.csv
"""

import pandas as pd
import numpy as np
import random
import re
import os
import sys
import calendar
from datetime import date, timedelta
from collections import defaultdict

# ============================================================
# 0.  FIXED SEED — must be set before ANY random call
# ============================================================
MASTER_SEED = 42
np.random.seed(MASTER_SEED)
random.seed(MASTER_SEED)

# ============================================================
# 1.  LOAD DIMENSION FILES
# ============================================================
os.chdir(r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data")

print("Loading dimension files …")
REQUIRED_FILES = ["dim_retailer.csv", "dim_distributor.csv", "dim_sku.csv", "dim_territory.csv"]
for f in REQUIRED_FILES:
    if not os.path.exists(f):
        print(f"ERROR: {f} not found in current directory. Aborting.")
        sys.exit(1)

dim_retailer    = pd.read_csv("dim_retailer.csv")
dim_distributor = pd.read_csv("dim_distributor.csv")
dim_sku         = pd.read_csv("dim_sku.csv")
dim_territory   = pd.read_csv("dim_territory.csv")

# Normalise column names
for d in [dim_retailer, dim_distributor, dim_sku, dim_territory]:
    d.columns = [c.strip().lower().replace(" ", "_") for c in d.columns]

# ============================================================
# 2.  BUILD LOOKUP STRUCTURES
# ============================================================

# ── territory → state ──────────────────────────────────────
def state_from_tid(tid: str) -> str:
    t = str(tid).upper()
    if "WB" in t:  return "West Bengal"
    if "OD" in t:  return "Odisha"
    if "AS" in t:  return "Assam"
    return "Unknown"

if "state" in dim_territory.columns:
    terr_state: dict[str, str] = dict(zip(dim_territory["territory_id"], dim_territory["state"]))
else:
    terr_state = {r["territory_id"]: state_from_tid(r["territory_id"])
                  for _, r in dim_territory.iterrows()}

# ── distributor info ────────────────────────────────────────
_name_col = next((c for c in dim_distributor.columns if "name" in c), None)
dist_info: dict[str, dict] = {}
for _, r in dim_distributor.iterrows():
    dist_info[r["distributor_id"]] = {
        "tier":         str(r["distributor_tier"]).strip(),
        "territory_id": str(r["territory_id"]).strip(),
        "name":         str(r[_name_col]) if _name_col else str(r["distributor_id"]),
    }

# ── retailer info ───────────────────────────────────────────
_deact_col = next((c for c in dim_retailer.columns
                   if "deact" in c or "deactivation" in c), None)
_status_col = next((c for c in dim_retailer.columns if "status" in c), None)
_terr_col_r = next((c for c in dim_retailer.columns if "territory" in c), None)

ret_info: dict[str, dict] = {}
for _, r in dim_retailer.iterrows():
    rid  = str(r["retailer_id"]).strip()
    did  = str(r["distributor_id"]).strip()
    tid  = str(r[_terr_col_r]).strip() if _terr_col_r else dist_info.get(did, {}).get("territory_id", "")
    on   = pd.to_datetime(r["onboard_date"]).date()
    stat = str(r[_status_col]).strip() if _status_col else "Active"
    if _deact_col:
        _dv = str(r.get(_deact_col, "")).strip().lower()
        if _dv and _dv not in ("nan", "none", "nat", "null", ""):
            deact = pd.to_datetime(_dv).date()
        else:
            deact = date(2099, 12, 31)
    else:
        deact = date(2099, 12, 31)
    ret_info[rid] = {
        "distributor_id":    did,
        "territory_id":      tid,
        "onboard_date":      on,
        "status":            stat,
        "deactivation_date": deact,
    }

# ── sku info ────────────────────────────────────────────────
_pack_col = next((c for c in dim_sku.columns if "pack" in c), None)
sku_info: dict[str, dict] = {}
for _, r in dim_sku.iterrows():
    sid = str(r["sku_id"]).strip()
    pack_raw = r[_pack_col] if _pack_col else 0
    try:
        pack_val = float(re.sub(r"[^0-9.]", "", str(pack_raw)))
    except Exception:
        pack_val = 0.0
    sku_info[sid] = {
        "category":      str(r["category"]).strip(),
        "pack_size":     pack_val,
        "retailer_price": float(r["retailer_price"]),
        "launch_fy":     str(r["launch_fy"]).strip(),
    }

# ── distributor → [retailer_ids] ───────────────────────────
dist_retailers: dict[str, list] = defaultdict(list)
for rid, ri in ret_info.items():
    dist_retailers[ri["distributor_id"]].append(rid)

# ── territory → first onboard date (for Anomaly 2 month-offset) ─
terr_first_onboard: dict[str, date] = {}
for rid, ri in ret_info.items():
    tid = ri["territory_id"]
    if tid not in terr_first_onboard or ri["onboard_date"] < terr_first_onboard[tid]:
        terr_first_onboard[tid] = ri["onboard_date"]

# ============================================================
# 3.  CONSTANTS
# ============================================================
FY_RANGES = {
    "FY2023-24": (date(2023, 4, 15), date(2024, 3, 31)),
    "FY2024-25": (date(2024, 4,  1), date(2025, 3, 31)),
    "FY2025-26": (date(2025, 4,  1), date(2026, 3, 31)),
}
FY_ORDER_MAP = {"FY2023-24": 1, "FY2024-25": 2, "FY2025-26": 3}

# Bill-number profile assignments
PROFILE_1 = {"DIST-001","DIST-004","DIST-008","DIST-010","DIST-012","DIST-024",
             "DIST-026","DIST-028","DIST-032","DIST-034"}
PROFILE_2 = {"DIST-002","DIST-009","DIST-011","DIST-013","DIST-017",
             "DIST-018","DIST-020","DIST-022","DIST-025","DIST-027",
             "DIST-029","DIST-030"}
PROFILE_3 = {"DIST-003","DIST-005","DIST-006","DIST-007","DIST-014",
             "DIST-015","DIST-019","DIST-031","DIST-033"}
PROFILE_4 = {"DIST-016","DIST-021","DIST-023","DIST-035","DIST-036"}
PROFILE_4_STARTS = {
    "DIST-016": 100_000, "DIST-021": 200_000, "DIST-023": 300_000,
    "DIST-035": 400_000, "DIST-036": 500_000,
}

STATE_BASE_TARGET = {
    "FY2023-24": {"West Bengal": 1_140, "Odisha": 1_000, "Assam":   925},
    "FY2024-25": {"West Bengal": 1_530, "Odisha": 1_370, "Assam": 1_290},
    "FY2025-26": {"West Bengal": 2_890, "Odisha": 2_550, "Assam": 2_360},
}
TWB08_BASE_TARGET = {
    "FY2024-25": int(1_530 * 0.80),   # 1_224
    "FY2025-26": int(2_890 * 0.80),   # 2_312
}

GRADE_WEIGHTS = {"A": 3.20, "B": 0.90, "C": 0.10}

YEAR2_SKUS = {
    "SPC-PPF-0050G","SWT-JGB-0500G","SWT-JGP-0500G",
    "FLR-BSN-0500G","FLR-BSN-1000G",
}
YEAR1_SKUS_TWB08 = {
    "OIL-MUS-0500M","OIL-MUS-1000M","OIL-MUS-5000M",
    "SPC-TUR-0100G","SPC-TUR-0200G","SPC-RCP-0100G","SPC-RCP-0200G",
    "SPC-COR-0100G","SPC-COR-0200G","SPC-PNP-0050G","SPC-PNP-0100G",
    "SPC-KKM-0050G","SPC-KKM-0100G",
}
ODISHA_ANOMALY_TERRS = {"T-OD-01","T-OD-02","T-OD-03"}
ANOMALY2_SKUS        = {"SPC-PPF-0050G","SPC-KKM-0050G","SPC-KKM-0100G"}
ASSAM_MONSOON_OILS   = {"OIL-MUS-5000M","OIL-MUS-1000M"}
MUSTARD_OIL_SKUS     = {"OIL-MUS-0500M","OIL-MUS-1000M","OIL-MUS-5000M"}

# ============================================================
# 4.  STORE-SIZE GRADE ASSIGNMENT  (deterministic)
# ============================================================
_grade_rng = np.random.RandomState(MASTER_SEED)
_all_ret_sorted = sorted(ret_info.keys())
_grades_arr = _grade_rng.choice(
    ["A","B","C"], size=len(_all_ret_sorted), p=[0.20, 0.35, 0.45])
store_weight_map = {r: GRADE_WEIGHTS[_grades_arr[i]] for i, r in enumerate(_all_ret_sorted)}

# ============================================================
# 5.  BILL-NUMBER COUNTERS
# ============================================================
p1_fy_ctr:   dict[str, dict] = {d: {} for d in PROFILE_1}   # d → {fy_key: n}
p2_date_ctr: dict[str, dict] = {d: {} for d in PROFILE_2}   # d → {date_str: n}
p3_ctr:      dict[str, int]  = {d: 0 for d in PROFILE_3}
p4_ctr:      dict[str, int]  = dict(PROFILE_4_STARTS)

# ============================================================
# 6.  HELPER FUNCTIONS
# ============================================================

def get_fy_key(d: date) -> str:
    if d.month >= 4:
        return f"FY{d.year}-{str(d.year+1)[-2:]}"
    return f"FY{d.year-1}-{str(d.year)[-2:]}"

def get_fy_yy(d: date) -> str:
    """YY-YY suffix for Profile-1 bill numbers."""
    if d.month >= 4:
        return f"{str(d.year)[-2:]}-{str(d.year+1)[-2:]}"
    return f"{str(d.year-1)[-2:]}-{str(d.year)[-2:]}"

def working_days_in_range(start: date, end: date) -> list:
    days, d = [], start
    while d <= end:
        if d.weekday() != 6:   # 6 = Sunday
            days.append(d)
        d += timedelta(days=1)
    return days

def next_workday(d: date) -> date:
    while d.weekday() == 6:
        d += timedelta(days=1)
    return d

def get_initials(name: str) -> str:
    words = str(name).strip().split()
    return "".join(w[0].upper() for w in words[:3] if w)

def gen_bill_no(dist_id: str, bill_date: date, dist_name: str) -> str:
    if dist_id in PROFILE_1:
        fy_key = get_fy_key(bill_date)
        fy_yy  = get_fy_yy(bill_date)
        p1_fy_ctr[dist_id][fy_key] = p1_fy_ctr[dist_id].get(fy_key, 0) + 1
        initials = get_initials(dist_name)
        return f"{initials}/{fy_yy}/{p1_fy_ctr[dist_id][fy_key]:05d}"

    if dist_id in PROFILE_2:
        num   = dist_id.split("-")[1]          # "002", "009", etc.
        short = f"D{num}"
        ds    = bill_date.strftime("%Y%m%d")
        p2_date_ctr[dist_id][ds] = p2_date_ctr[dist_id].get(ds, 0) + 1
        return f"INV-{short}-{ds}-{p2_date_ctr[dist_id][ds]:03d}"

    if dist_id in PROFILE_3:
        p3_ctr[dist_id] += 1
        return f"BILL{p3_ctr[dist_id]:06d}"

    # Profile 4 — manual cash memo with gaps
    p4_ctr[dist_id] += random.randint(1, 20)
    return str(p4_ctr[dist_id])


def get_cat_weights(fy_key: str, territory_id: str) -> dict:
    """Category weights for SKU selection."""
    if territory_id == "T-WB-08" or fy_key == "FY2023-24":
        return {"Edible Oils": 0.60, "Spices & Masalas": 0.40,
                "Natural Sweeteners": 0.0, "Flours & Grain Staples": 0.0}
    return {"Edible Oils": 0.40, "Spices & Masalas": 0.40,
            "Natural Sweeteners": 0.10, "Flours & Grain Staples": 0.10}


def seasonal_mult(bill_date: date, state: str, category: str) -> float:
    m  = bill_date.month
    fy = get_fy_key(bill_date)

    if state == "West Bengal":
        if m == 10:
            if fy == "FY2025-26":
                # Both Durga Puja (Oct) and Diwali (Oct FY26): max per category
                dp = {"Edible Oils": 1.45, "Spices & Masalas": 1.15,
                      "Natural Sweeteners": 1.25, "Flours & Grain Staples": 1.25}
                di = {"Edible Oils": 1.30, "Spices & Masalas": 1.10,
                      "Natural Sweeteners": 1.55, "Flours & Grain Staples": 1.55}
                return max(dp.get(category, 1.0), di.get(category, 1.0))
            else:
                # Durga Puja only
                dp = {"Edible Oils": 1.45, "Spices & Masalas": 1.15,
                      "Natural Sweeteners": 1.25, "Flours & Grain Staples": 1.25}
                return dp.get(category, 1.0)

        if m == 11 and fy in ("FY2023-24", "FY2024-25"):
            # Diwali (Nov in FY24, FY25)
            di = {"Edible Oils": 1.30, "Spices & Masalas": 1.10,
                  "Natural Sweeteners": 1.55, "Flours & Grain Staples": 1.55}
            return di.get(category, 1.0)

        if m == 12 and fy in ("FY2023-24", "FY2024-25"):
            return 0.85   # post-Diwali hangover

        if m == 11 and fy == "FY2025-26":
            return 0.85   # post-Diwali hangover (Diwali was in Oct)

        if m in (7, 8):
            return 0.90   # monsoon dip (WB only)

    elif state == "Odisha":
        if m == 10:        return 1.05
        if m in (7, 8):   return 0.90   # monsoon dip

    elif state == "Assam":
        # Bihu: April 1–15 only
        if m == 4 and bill_date.day <= 15:
            bh = {"Edible Oils": 1.60, "Spices & Masalas": 1.20,
                  "Natural Sweeteners": 1.30, "Flours & Grain Staples": 1.30}
            return bh.get(category, 1.0)
        # Post-Bihu hangover: May
        if m == 5:
            return 0.85
        # NOTE: monsoon dip (0.90×) does NOT apply to Assam ordered_qty

    return 1.0


def fill_rate_range(state: str, bill_date: date, sku_id: str) -> tuple:
    m  = bill_date.month
    fy = get_fy_key(bill_date)

    if state == "West Bengal":
        return (0.95, 0.99)

    if state == "Odisha":
        return (0.90, 0.95)

    if state == "Assam":
        # Monsoon months in FY2025-26 only
        if fy == "FY2025-26" and m in (6, 7, 8, 9):
            if sku_id in ASSAM_MONSOON_OILS:
                return (0.55, 0.60)
            return (0.55, 0.65)
        # Bihu April 1–15 oil pre-order
        if m == 4 and bill_date.day <= 15 and sku_id in ASSAM_MONSOON_OILS:
            return (0.60, 0.70)
        return (0.82, 0.90)

    return (0.90, 0.95)


def expected_fill_rate(state: str, bill_date: date, sku_id: str) -> float:
    lo, hi = fill_rate_range(state, bill_date, sku_id)
    return (lo + hi) / 2.0


def sku_size_weights(sku_list: list) -> dict:
    """60% probability mass on smaller pack sizes, 40% on larger."""
    if not sku_list:
        return {}
    if len(sku_list) == 1:
        return {sku_list[0]: 1.0}
    sizes = sorted([(s, sku_info.get(s, {}).get("pack_size", 0)) for s in sku_list],
                   key=lambda x: x[1])
    mid = max(1, len(sizes) // 2)
    small_n = mid
    large_n = max(1, len(sizes) - mid)
    w = {}
    for i, (s, _) in enumerate(sizes):
        w[s] = 0.60 / small_n if i < mid else 0.40 / large_n
    tot = sum(w.values())
    return {s: v / tot for s, v in w.items()}


def get_eligible_skus_by_cat(fy_key: str, territory_id: str) -> dict:
    """Returns {category: [sku_id, …]} for eligible SKUs."""
    fy_num = FY_ORDER_MAP[fy_key]
    is_twb08 = (territory_id == "T-WB-08")
    result: dict[str, list] = defaultdict(list)

    for sid, si in sku_info.items():
        launch_num = FY_ORDER_MAP.get(si["launch_fy"], 99)
        if launch_num > fy_num:
            continue
        cat = si["category"]
        # FY2023-24: only Oils & Spices (Sweeteners/Flours not launched yet)
        if fy_key == "FY2023-24" and cat not in ("Edible Oils", "Spices & Masalas"):
            continue
        # T-WB-08 (FY24-25, FY25-26): only Year-1 SKUs
        if is_twb08 and fy_key in ("FY2024-25", "FY2025-26") and sid not in YEAR1_SKUS_TWB08:
            continue
        result[cat].append(sid)

    return dict(result)


# ============================================================
# 7.  MAIN GENERATION LOOP
# ============================================================
_rng = np.random.RandomState(MASTER_SEED + 7)   # separate RandomState for quantities

all_rows: list = []
rows_gen = 0

COLS = [
    "distributor_bill_no", "line_item_no", "bill_date",
    "retailer_id", "distributor_id", "sku_id",
    "ordered_qty", "delivered_qty", "returned_qty", "bill_value_inr",
]

for fy_key, (fy_start, fy_end) in FY_RANGES.items():
    print(f"\n{'═'*60}\n  Generating {fy_key}  ({fy_start} → {fy_end})\n{'═'*60}")

    # Build list of (year, month) pairs in this FY
    fy_months: list[tuple] = []
    _c = date(fy_start.year, fy_start.month, 1)
    while _c <= fy_end:
        fy_months.append((_c.year, _c.month))
        _c = date(_c.year + (_c.month == 12), (_c.month % 12) + 1, 1)

    for dist_id in sorted(dist_info.keys()):
        di        = dist_info[dist_id]
        tier      = di["tier"]
        terr_id   = di["territory_id"]
        state     = terr_state.get(terr_id, state_from_tid(terr_id))
        dist_name = di["name"]
        is_twb08  = (terr_id == "T-WB-08")
        is_od_anom = (terr_id in ODISHA_ANOMALY_TERRS)

        retailers = dist_retailers.get(dist_id, [])
        if not retailers:
            continue

        # Eligible SKUs for this FY + territory
        elig = get_eligible_skus_by_cat(fy_key, terr_id)
        cw_raw = get_cat_weights(fy_key, terr_id)
        active_cats = {c: w for c, w in cw_raw.items()
                       if w > 0 and c in elig and elig[c]}
        if not active_cats:
            continue
        _tw = sum(active_cats.values())
        norm_cw = {c: w / _tw for c, w in active_cats.items()}

        # Build flat weighted SKU list for np.random.choice
        all_pool: list[str] = []
        all_pool_w: list[float] = []
        for c, cw_val in norm_cw.items():
            skus_in_cat = elig[c]
            sw = sku_size_weights(skus_in_cat)
            for s in skus_in_cat:
                all_pool.append(s)
                all_pool_w.append(cw_val * sw.get(s, 1.0 / len(skus_in_cat)))
        if not all_pool:
            continue
        _warr = np.array(all_pool_w, dtype=float)
        _warr /= _warr.sum()

        # Base revenue target per store per month
        base_target = STATE_BASE_TARGET.get(fy_key, {}).get(state, 2_000)
        if is_twb08 and fy_key in ("FY2024-25", "FY2025-26"):
            base_target = TWB08_BASE_TARGET.get(fy_key, int(base_target * 0.80))

        # SKUs-per-bill baseline
        _skus_per_bill = {"A": 6, "B": 5}.get(tier, 3)

        # ── Month loop ─────────────────────────────────────
        for yr, mo in fy_months:
            mo_start = date(yr, mo, 1)
            _, last_day = calendar.monthrange(yr, mo)
            mo_end = date(yr, mo, last_day)

            eff_start = max(mo_start, fy_start)
            eff_end   = min(mo_end, fy_end)
            if eff_start > eff_end:
                continue

            is_april2023 = (yr == 2023 and mo == 4)
            april_prorate = 0.5 if is_april2023 else 1.0

            # ── Retailer loop ───────────────────────────────
            for ret_id in retailers:
                ri = ret_info[ret_id]
                onboard = ri["onboard_date"]
                deact   = ri["deactivation_date"]

                # Basic eligibility
                if onboard > eff_end:
                    continue
                if deact <= eff_start:
                    continue

                ret_start = max(eff_start, onboard)
                ret_end   = min(eff_end, deact - timedelta(days=1))

                # For inactive retailers: enforce 30-day buffer before deactivation
                if ri["status"].lower() == "inactive":
                    window_days = (deact - onboard).days
                    if window_days >= 35:
                        cap = deact - timedelta(days=30)
                        ret_end = min(ret_end, cap)

                if ret_start > ret_end:
                    continue

                wdays = working_days_in_range(ret_start, ret_end)
                if not wdays:
                    continue

                # Bills per month
                if tier == "A":
                    n_bills = 1 if (is_twb08 and fy_key in ("FY2024-25","FY2025-26")) \
                              else random.choices([2, 3], weights=[60, 40])[0]
                elif tier == "B":
                    n_bills = random.choices([1, 2], weights=[65, 35])[0]
                else:
                    n_bills = 1

                n_bills = min(n_bills, len(wdays))
                if n_bills == 0:
                    continue

                # Generate bill dates with ≥7-day gap
                first_month = (onboard.year == yr and onboard.month == mo)
                if n_bills == 1:
                    bill_dates = [random.choice(wdays)]
                else:
                    mid_date = date(yr, mo, 15)
                    if first_month:
                        first_pool = wdays
                    else:
                        first_pool = [d for d in wdays if d <= mid_date] or wdays

                    first_bd = random.choice(first_pool)
                    bill_dates = [first_bd]

                    for _ in range(n_bills - 1):
                        gap  = random.randint(7, 10)
                        nxt  = next_workday(bill_dates[-1] + timedelta(days=gap))
                        if nxt > ret_end:
                            # cap to last valid working day
                            after = [d for d in wdays if d > bill_dates[-1]]
                            nxt = after[-1] if after else None
                        if nxt is None or nxt <= bill_dates[-1] or nxt > ret_end:
                            break
                        bill_dates.append(nxt)

                cad_frac = 1.0 / len(bill_dates)

                # ── Bill loop ───────────────────────────────
                for bill_date in bill_dates:
                    # Salary week: days 1–5 of the month
                    sal_mult = 1.15 if (bill_date.day <= 5 or bill_date.day >= 28) else 1.0

                    # Number of SKUs
                    n_skus = random.choice([3, 4]) if tier == "C" else _skus_per_bill
                    n_pick = min(n_skus, len(all_pool))
                    if n_pick == 0:
                        continue

                    try:
                        selected = list(_rng.choice(all_pool, size=n_pick,
                                                    replace=False, p=_warr))
                    except Exception:
                        selected = all_pool[:n_pick]

                    bill_no = gen_bill_no(dist_id, bill_date, dist_name)

                    # ── SKU / line-item loop ────────────────
                    for line_no, sku_id in enumerate(selected, 1):
                        if sku_id not in sku_info:
                            continue
                        si2   = sku_info[sku_id]
                        cat   = si2["category"]
                        price = si2["retailer_price"]

                        seas   = seasonal_mult(bill_date, state, cat)
                        cw_val = norm_cw.get(cat, 0.0)
                        sw_d   = sku_size_weights(elig.get(cat, [sku_id]))
                        sku_w  = sw_d.get(sku_id, 1.0)

                        efr = expected_fill_rate(state, bill_date, sku_id)

                        # Quantity derivation formula
                        base_mo = base_target * store_weight_map[ret_id] * cad_frac * april_prorate
                        cat_alloc = base_mo * cw_val * seas
                        sku_alloc = cat_alloc * sku_w
                        noise     = _rng.uniform(0.90, 1.10)
                        t_del     = sku_alloc / max(price, 0.01) * noise
                        ord_q     = round(t_del / max(efr, 0.01))
                        ord_q     = max(1, round(ord_q * sal_mult))

                        # Delivered qty (fill rate)
                        fr_lo, fr_hi = fill_rate_range(state, bill_date, sku_id)
                        fr    = _rng.uniform(fr_lo, fr_hi)
                        del_q = max(0, round(ord_q * fr))

                        # Assam monsoon: del_q=0 is valid; elsewhere ensure ≥1
                        is_assam_monsoon = (
                            state == "Assam"
                            and get_fy_key(bill_date) == "FY2025-26"
                            and bill_date.month in (6, 7, 8, 9)
                        )
                        if del_q == 0 and not is_assam_monsoon:
                            del_q = 1
                        if del_q > ord_q:
                            del_q = ord_q

                        # returned_qty
                        ret_q = 0
                        if del_q > 0:
                            if is_od_anom and sku_id in ANOMALY2_SKUS:
                                tl_date = terr_first_onboard.get(terr_id, fy_start)
                                m_off = ((bill_date.year - tl_date.year) * 12
                                         + bill_date.month - tl_date.month + 1)
                                if sku_id == "SPC-PPF-0050G":
                                    if m_off >= 3:
                                        rate = _rng.uniform(0.18, 0.25)
                                        if del_q < 4:
                                            ret_q = 1 if random.random() < rate else 0
                                        else:
                                            ret_q = round(del_q * rate)
                                else:  # KKM skus
                                    if m_off >= 2:
                                        rate = _rng.uniform(0.22, 0.30)
                                        if del_q < 4:
                                            ret_q = 1 if random.random() < 0.26 else 0
                                        else:
                                            ret_q = round(del_q * rate)

                            elif state == "West Bengal" and sku_id in ANOMALY2_SKUS:
                                r_v = random.random()
                                if r_v <= 0.05:
                                    ret_q = max(1, round(del_q * _rng.uniform(0.01, 0.03)))

                            else:
                                r_v = random.random()
                                if r_v <= 0.05:
                                    ret_q = max(1, round(del_q * _rng.uniform(0.01, 0.02)))

                        ret_q     = min(ret_q, del_q)
                        bill_val  = round(del_q * price)

                        all_rows.append([
                            bill_no, line_no,
                            bill_date.strftime("%Y-%m-%d"),
                            ret_id, dist_id, sku_id,
                            int(ord_q), int(del_q), int(ret_q), int(bill_val),
                        ])
                        rows_gen += 1
                        if rows_gen % 50_000 == 0:
                            print(f"  Progress: {rows_gen:,} rows generated…")

print(f"\nBase generation complete: {rows_gen:,} rows")

# ============================================================
# 8.  ERROR INJECTION
# ============================================================
print("\nRunning error injection…")

# Re-seed for reproducible injection
np.random.seed(MASTER_SEED)
random.seed(MASTER_SEED)

df = pd.DataFrame(all_rows, columns=COLS)
n  = len(df)           # base row count (before appending duplicates)
ALL_IDX = np.arange(n)

# Track injection sets (index-based)
err_rows: dict[int, set] = {i: set() for i in range(1, 15)}

# ── Error 1: Leading/trailing whitespace ──────────────────
n1_ret = round(n * 0.010)
n1_sku = round(n * 0.005)
e1_ret = set(map(int, np.random.choice(ALL_IDX, size=n1_ret, replace=False)))
_pool1 = np.array([i for i in ALL_IDX if i not in e1_ret])
e1_sku = set(map(int, np.random.choice(_pool1, size=min(n1_sku, len(_pool1)), replace=False)))

for i in e1_ret:
    df.at[i, "retailer_id"] = str(df.at[i, "retailer_id"]) + " "
    err_rows[1].add(i)
for i in e1_sku:
    df.at[i, "sku_id"] = " " + str(df.at[i, "sku_id"])
    err_rows[1].add(i)

# ── Error 2: Case sensitivity ─────────────────────────────
n2     = round(n * 0.008)
n2_ret = n2 // 2
n2_sku = n2 - n2_ret
_avail2 = np.array([i for i in ALL_IDX if i not in err_rows[1]])
e2_ret  = set(map(int, np.random.choice(_avail2, size=min(n2_ret, len(_avail2)), replace=False)))
_avail2b = np.array([i for i in _avail2 if i not in e2_ret])
e2_sku  = set(map(int, np.random.choice(_avail2b, size=min(n2_sku, len(_avail2b)), replace=False)))

for i in e2_ret:
    df.at[i, "retailer_id"] = str(df.at[i, "retailer_id"]).lower()
    err_rows[2].add(i)
for i in e2_sku:
    df.at[i, "sku_id"] = str(df.at[i, "sku_id"]).lower()
    err_rows[2].add(i)

# ── Error 9: New-Year date typo ───────────────────────────
_p4_jan24 = df.iloc[:n].index[
    df.iloc[:n]["distributor_id"].isin(PROFILE_3) &
    df.iloc[:n]["bill_date"].astype(str).str.startswith("2024-01")
].tolist()
_e9_cnt = min(12, len(_p4_jan24))
if _e9_cnt > 0:
    _e9_idx = np.random.choice(_p4_jan24, size=_e9_cnt, replace=False)
    for i in _e9_idx:
        df.at[i, "bill_date"] = str(df.at[i, "bill_date"]).replace("2024-01", "2023-01")
        err_rows[9].add(int(i))

# ── Error 3: Mixed date formats ───────────────────────────
n3_dmy = round(n * 0.03)
n3_mdy = round(n * 0.02)
_pool3  = np.array([i for i in ALL_IDX])
e3_dmy  = set(map(int, np.random.choice(_pool3, size=min(n3_dmy, len(_pool3)), replace=False)))
_pool3b = np.array([i for i in _pool3 if i not in e3_dmy])
e3_mdy  = set(map(int, np.random.choice(_pool3b, size=min(n3_mdy, len(_pool3b)), replace=False)))

for i in e3_dmy:
    s = str(df.at[i, "bill_date"])
    try:
        d_obj = date.fromisoformat(s)
        df.at[i, "bill_date"] = d_obj.strftime("%d/%m/%Y")
    except Exception:
        pass
    err_rows[3].add(i)

for i in e3_mdy:
    s = str(df.at[i, "bill_date"])
    try:
        d_obj = date.fromisoformat(s)
        df.at[i, "bill_date"] = d_obj.strftime("%m-%d-%Y")
    except Exception:
        pass
    err_rows[3].add(i)

# ── Error 4: CASH-SALE (0.3% — Profile-4 / Tier-C only) ──
n4 = round(n * 0.003)
_p4_idx = np.where(df["distributor_id"].isin(PROFILE_4).values)[0]
e4_idx  = set(map(int, np.random.choice(_p4_idx, size=min(n4, len(_p4_idx)), replace=False)))
for i in e4_idx:
    df.at[i, "retailer_id"] = "CASH-SALE"
    err_rows[4].add(i)

# ── Error 6: Impossible returns (0.2%) ───────────────────
# Done before Error 5 so we can exclude from error-5 check
n6      = round(n * 0.002)
_e6_cands = np.where(df["delivered_qty"].values >= 2)[0]
e6_idx  = set(map(int, np.random.choice(_e6_cands, size=min(n6, len(_e6_cands)), replace=False)))
for i in e6_idx:
    df.at[i, "returned_qty"] = int(df.at[i, "delivered_qty"]) + random.randint(1, 3)
    err_rows[6].add(i)

# ── Error 11: Free goods / zero bill value (0.2%) ────────
n11 = round(n * 0.002)
_wb_tiera_dists = {d for d, di2 in dist_info.items()
                   if di2["tier"] == "A"
                   and terr_state.get(di2["territory_id"], "") == "West Bengal"}
_e11_mask = (df["distributor_id"].isin(_wb_tiera_dists)
             & df["sku_id"].isin(MUSTARD_OIL_SKUS)
             & (df["delivered_qty"] >= 1))
_e11_cands = np.where(_e11_mask.values)[0]
if len(_e11_cands) < n11:
    # Fallback: any row with del_qty >= 1
    _e11_cands = np.where(df["delivered_qty"].values >= 1)[0]
e11_idx = set(map(int, np.random.choice(_e11_cands, size=min(n11, len(_e11_cands)), replace=False)))
for i in e11_idx:
    df.at[i, "bill_value_inr"] = 0
    err_rows[11].add(i)

# ── Error 12: Negative delivery (1% of Tier-C line items) ─
_p4_all = np.where(df["distributor_id"].isin(PROFILE_4).values)[0]
n12 = round(len(_p4_all) * 0.01)
_e12_cands = [i for i in _p4_all if 1 <= int(df.at[i, "delivered_qty"]) <= 5]
e12_idx = set(map(int, np.random.choice(_e12_cands, size=min(n12, len(_e12_cands)), replace=False)))
for i in e12_idx:
    df.at[i, "delivered_qty"]  = random.randint(-5, -1)
    df.at[i, "bill_value_inr"] = 0
    df.at[i, "returned_qty"]   = 0
    err_rows[12].add(i)

# ── Error 13: Orphaned SKU IDs (0.5% — Tier-C) ───────────
n13 = round(n * 0.005)
_e13_cands = [i for i in _p4_all if i not in err_rows[2]]
e13_idx = set(map(int, np.random.choice(_e13_cands, size=min(n13, len(_e13_cands)), replace=False)))
for i in e13_idx:
    df.at[i, "sku_id"]         = "SKU-PROMO-01" if random.random() < 0.6 else "MISC-ITEM"
    df.at[i, "bill_value_inr"] = 0
    err_rows[13].add(i)

# ── Error 5: Broken bill value (0.5%, exclude 11/12/13) ──
_excl5 = err_rows[11] | err_rows[12] | err_rows[13]
n5 = round(n * 0.005)
_e5_cands = np.array([i for i in ALL_IDX if i not in _excl5])
e5_idx = set(map(int, np.random.choice(_e5_cands, size=min(n5, len(_e5_cands)), replace=False)))
_e5_offsets = [-50, -30, -20, 20, 30, 50, 75, 100]
for i in e5_idx:
    df.at[i, "bill_value_inr"] = int(df.at[i, "bill_value_inr"]) + random.choice(_e5_offsets)
    err_rows[5].add(i)

# Case-insensitive SKU price lookup (needed for Error 8 which may read error-modified rows)
sku_price_ci: dict[str, float] = {k.lower(): v["retailer_price"] for k, v in sku_info.items()}

# ── Error 7: Exact duplicate rows (250) ──────────────────
_e7_src = np.random.choice(ALL_IDX, size=250, replace=False)
_dup7   = df.iloc[_e7_src].copy()
_e7_start = len(df)
err_rows[7] = set(range(_e7_start, _e7_start + 250))
df = pd.concat([df, _dup7], ignore_index=True)

# ── Error 8: Partial duplicates — Profile-2 (60) ─────────
_p2_base = np.where(df.iloc[:n]["distributor_id"].isin(PROFILE_2).values)[0]
_e8_src  = np.random.choice(_p2_base, size=min(60, len(_p2_base)), replace=False)
_dup8_rows = []
for idx in _e8_src:
    row = df.iloc[idx].copy()
    delta = random.randint(1, 5)
    new_oq = int(row["ordered_qty"]) + delta
    new_dq = new_oq   # adjusted so delivered ≤ ordered
    row["ordered_qty"]    = new_oq
    row["delivered_qty"]  = new_dq
    _sku_raw = str(row["sku_id"]).strip().lower()
    _price_lookup = sku_price_ci.get(_sku_raw)
    if _price_lookup:
        row["bill_value_inr"] = round(new_dq * _price_lookup)
    else:
        # Price unknown (orphaned sku from Err13); scale proportionally
        old_bv = int(row["bill_value_inr"])
        old_dq = int(row["delivered_qty"]) - delta   # original delivered
        row["bill_value_inr"] = round(old_bv * new_dq / max(old_dq, 1))
    _dup8_rows.append(row)

_dup8_df = pd.DataFrame(_dup8_rows, columns=df.columns)
_e8_start = len(df)
df = pd.concat([df, _dup8_df], ignore_index=True)
err_rows[8] = set(range(_e8_start, _e8_start + len(_dup8_rows)))

# ── Error 10: UOM outlier (0.10% of base rows) ───────────
n10 = round(n * 0.001)
e10_idx = set(map(int, np.random.choice(ALL_IDX, size=min(n10, n), replace=False)))
for i in e10_idx:
    mult = random.choice([12, 24, 48])
    new_oq = int(df.at[i, "ordered_qty"])  * mult
    new_dq = int(df.at[i, "delivered_qty"]) * mult
    new_rq = int(df.at[i, "returned_qty"])  * mult
    _sid   = str(df.at[i, "sku_id"]).strip()
    _price = sku_info.get(_sid, {}).get("retailer_price", None)
    df.at[i, "ordered_qty"]   = new_oq
    df.at[i, "delivered_qty"] = new_dq
    df.at[i, "returned_qty"]  = new_rq
    if _price:
        df.at[i, "bill_value_inr"] = round(new_dq * _price)
    else:
        df.at[i, "bill_value_inr"] = int(df.at[i, "bill_value_inr"]) * mult
    err_rows[10].add(i)

# ── Error 14: Epoch / future date glitches (exactly 8) ───
_p3_cands14 = df.iloc[:n].index[df.iloc[:n]["distributor_id"].isin(PROFILE_3)].tolist()
_p2_cands14 = df.iloc[:n].index[df.iloc[:n]["distributor_id"].isin(PROFILE_2)].tolist()
_e14_p3 = list(np.random.choice(_p3_cands14, size=min(4, len(_p3_cands14)), replace=False))
_e14_p2 = list(np.random.choice(_p2_cands14, size=min(4, len(_p2_cands14)), replace=False))
for i in _e14_p3:
    df.at[i, "bill_date"] = "1970-01-01"
    err_rows[14].add(int(i))
for i in _e14_p2:
    df.at[i, "bill_date"] = "2099-12-31"
    err_rows[14].add(int(i))

print(f"Error injection complete. Total rows: {len(df):,}")

# ============================================================
# 9.  WRITE CSV  (chunked)
# ============================================================
print("\nWriting fact_secondary_sales.csv …")

# Enforce integer types before writing
for col in ["line_item_no","ordered_qty","delivered_qty","returned_qty","bill_value_inr"]:
    df[col] = df[col].astype(int)

OUTPUT_FILE = "fact_secondary_sales.csv"
CHUNK = 50_000
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as fh:
    df.iloc[:min(CHUNK, len(df))].to_csv(fh, index=False)
    for start in range(CHUNK, len(df), CHUNK):
        end = min(start + CHUNK, len(df))
        df.iloc[start:end].to_csv(fh, index=False, header=False)
        print(f"  Progress: {end:,} rows written…")

print(f"\n✓ Wrote {len(df):,} rows → {OUTPUT_FILE}")

# ============================================================
# 10. VALIDATION REPORT
# ============================================================

# ── Robust date parser ──────────────────────────────────────
def _parse_date_str(s: str):
    s = str(s).strip()
    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    # DD/MM/YYYY
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if m:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    # MM-DD-YYYY
    m = re.match(r"^(\d{2})-(\d{2})-(\d{4})$", s)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    return None

def _fy_from_str(s: str) -> str:
    try:
        d_obj = _parse_date_str(str(s))
        if d_obj is None:
            return "Unknown"
        if d_obj.month >= 4:
            return f"FY{d_obj.year}-{str(d_obj.year+1)[-2:]}"
        return f"FY{d_obj.year-1}-{str(d_obj.year)[-2:]}"
    except Exception:
        return "Unknown"

# Work only on the base rows for revenue and anomaly checks
base = df.iloc[:n].copy()
base["_fy"]    = base["bill_date"].apply(_fy_from_str)
base["_state"] = base["distributor_id"].map(
    lambda d: terr_state.get(dist_info.get(d, {}).get("territory_id", ""), ""))
base["_month"] = base["bill_date"].apply(
    lambda s: ((_parse_date_str(str(s))) or date(1970, 1, 1)).strftime("%m")
)

print("\n" + "=" * 65)
print("=== FACT_SECONDARY_SALES VALIDATION REPORT ===")
print("=" * 65)
print(f"Total rows generated           : {len(df):,}")
print(f"Unique distributor_bill_no     : {df['distributor_bill_no'].nunique():,}")
print(f"Unique retailer_id (normalised): {df['retailer_id'].str.strip().nunique():,}")

# ── Annual revenue ──────────────────────────────────────────
FY_TARGETS = {
    "FY2023-24": (8_075_000,  8_925_000),
    "FY2024-25": (19_950_000, 22_050_000),
    "FY2025-26": (43_700_000, 48_300_000),
}
print("\n--- Annual Revenue Totals (bill_value_inr) ---")
for fy_lbl, (lo, hi) in FY_TARGETS.items():
    rev    = int(base[base["_fy"] == fy_lbl]["bill_value_inr"].sum())
    status = "PASS" if lo <= rev <= hi else "FAIL"
    print(f"{fy_lbl}: ₹{rev:>12,}  |  Target ₹{lo:,} – ₹{hi:,}  |  {status}")

# ── Anomaly 1 ───────────────────────────────────────────────
print("\n--- Anomaly 1 Validation (T-WB-08) ---")
twb08_rets_set = {r for r, ri in ret_info.items() if ri["territory_id"] == "T-WB-08"}
twb07_rets_set = {r for r, ri in ret_info.items() if ri["territory_id"] == "T-WB-07"}

_a1 = base[(base["retailer_id"].isin(twb08_rets_set)) & (base["_fy"] == "FY2024-25")]
y2_in_twb08 = _a1["sku_id"].isin(YEAR2_SKUS).sum()
print(f"Unique Year-2 SKUs billed in T-WB-08 FY2024-25 (must be 0) : {y2_in_twb08}")

def avg_bills_pm(sub_df, rets):
    rows = sub_df[sub_df["retailer_id"].str.strip().isin(rets)].copy()
    if rows.empty: return 0.0
    rows["_mo"] = rows["bill_date"].str[:7]
    # Count unique bill numbers (not line items) per retailer per month
    grp = rows.groupby(["retailer_id","_mo"])["distributor_bill_no"].nunique()
    return grp.mean()

twb08_bpm = avg_bills_pm(_a1, twb08_rets_set)
print(f"Avg bills/month/store T-WB-08 FY2024-25 (must be ~1.0)     : {twb08_bpm:.2f}")

_a1_07 = base[(base["retailer_id"].isin(twb07_rets_set)) & (base["_fy"] == "FY2024-25")]
twb07_bpm = avg_bills_pm(_a1_07, twb07_rets_set)
print(f"Avg bills/month/store T-WB-07 FY2024-25 (should be 1.5+)   : {twb07_bpm:.2f}")

# ── Anomaly 2 ───────────────────────────────────────────────
print("\n--- Anomaly 2 Validation (Odisha Returns) ---")
od_rets_set = {r for r, ri in ret_info.items() if ri["territory_id"] in ODISHA_ANOMALY_TERRS}
wb_rets_set = {r for r, ri in ret_info.items()
               if terr_state.get(ri["territory_id"], "") == "West Bengal"}

def return_pct(sub, ret_set, sku):
    rows = sub[(sub["retailer_id"].str.strip().isin(ret_set))
               & (sub["sku_id"].str.strip() == sku)
               & (sub["delivered_qty"] > 0)]
    if rows.empty: return 0.0
    return rows["returned_qty"].sum() / rows["delivered_qty"].sum() * 100

ppf_od = return_pct(base, od_rets_set, "SPC-PPF-0050G")
ppf_wb = return_pct(base, wb_rets_set, "SPC-PPF-0050G")
kkm_od = return_pct(base, od_rets_set, "SPC-KKM-0100G")
print(f"Avg return % SPC-PPF-0050G  T-OD-01/02/03 Month-3+ : {ppf_od:.1f}%")
print(f"Avg return % SPC-PPF-0050G  WB (must be <3%)        : {ppf_wb:.1f}%")
print(f"Avg return % SPC-KKM-0100G  T-OD-01/02/03 Month-2+ : {kkm_od:.1f}%")

# ── Anomaly 3 ───────────────────────────────────────────────
print("\n--- Anomaly 3 Validation (Assam Fill Rate) ---")
assam_rets_set = {r for r, ri in ret_info.items()
                  if terr_state.get(ri["territory_id"], "") == "Assam"}

def fill_rate_pct(sub):
    s = sub[sub["ordered_qty"] > 0]
    if s.empty: return 0.0
    return s["delivered_qty"].sum() / s["ordered_qty"].sum() * 100

wb_all   = base[base["_state"] == "West Bengal"]
as_nonm  = base[base["retailer_id"].str.strip().isin(assam_rets_set)
                & ~((base["_fy"] == "FY2025-26")
                    & base["_month"].isin(["06","07","08","09"]))]
as_mon   = base[base["retailer_id"].str.strip().isin(assam_rets_set)
                & (base["_fy"] == "FY2025-26")
                & base["_month"].isin(["06","07","08","09"])]
as_5k    = as_mon[as_mon["sku_id"].str.strip() == "OIL-MUS-5000M"]

print(f"Avg fill rate WB all months                    : {fill_rate_pct(wb_all):.1f}%")
print(f"Avg fill rate Assam non-monsoon                : {fill_rate_pct(as_nonm):.1f}%")
print(f"Avg fill rate Assam Jun-Sep FY2025-26          : {fill_rate_pct(as_mon):.1f}%")
print(f"Avg fill rate Assam Jun-Sep OIL-MUS-5000M      : {fill_rate_pct(as_5k):.1f}%")

# ── Data integrity ──────────────────────────────────────────
print("\n--- Data Integrity & Error Injection Validation ---")

_pos = base[base["delivered_qty"] > 0]
over_del = (_pos["delivered_qty"] > _pos["ordered_qty"]).sum()
print(f"Rows where delivered_qty > ordered_qty (must be 0)                        : {over_del}")

ret_gt_del = (base["returned_qty"] > base["delivered_qty"]).sum()
print(f"Rows where returned_qty > delivered_qty (must equal Error-6 + Error-12)   : {ret_gt_del}  (expected: {len(err_rows[6]) + len(err_rows[12])})")

# bill_date < onboard_date
_bad_date_cnt = 0
for _, row in base.iterrows():
    bd = _parse_date_str(str(row["bill_date"]))
    rid_clean = str(row["retailer_id"]).strip()
    if bd and rid_clean in ret_info:
        if bd < ret_info[rid_clean]["onboard_date"]:
            _bad_date_cnt += 1
print(f"Rows where bill_date < retailer onboard_date (Err-9 count + 4 epoch = 16) : {_bad_date_cnt}")

neg_del = (base["delivered_qty"] < 0).sum()
print(f"Rows with negative delivered_qty (must equal Error-12 count)              : {neg_del}  (injected: {len(err_rows[12])})")

orphan_sku = base["sku_id"].isin(["SKU-PROMO-01","MISC-ITEM"]).sum()
print(f"Rows with orphaned sku_id (must equal Error-13 count)                     : {orphan_sku}  (injected: {len(err_rows[13])})")

# Sunday check (only ISO-parseable dates)
_sunday = 0
for s in base["bill_date"]:
    try:
        if date.fromisoformat(str(s)).weekday() == 6:
            _sunday += 1
    except Exception:
        pass
print(f"Rows where bill_date is a Sunday (must be 0)                              : {_sunday}")

null_cnt = df.isnull().sum().sum()
print(f"Rows with any NULL value (must be 0, CASH-SALE is a string)               : {null_cnt}")

# Inactive retailer billed after deactivation
_inactive_after = 0
for _, row in base.iterrows():
    bd = _parse_date_str(str(row["bill_date"]))
    rid = str(row["retailer_id"]).strip()
    if bd and rid in ret_info:
        ri2 = ret_info[rid]
        if ri2["status"].lower() == "inactive" and bd >= ri2["deactivation_date"]:
            _inactive_after += 1
print(f"Rows where inactive retailer billed after deactivation_date (must be 0)  : {_inactive_after}")

# ── Raw data quality flags ──────────────────────────────────
print("\n--- Raw Data Quality Flags (Injected Errors — Verify Injection Worked) ---")
print(f"Rows with whitespace in retailer_id or sku_id (Error 1)                  : {len(err_rows[1])}")
print(f"Rows with lowercase retailer_id or sku_id (Error 2)                      : {len(err_rows[2])}")
print(f"Rows with non-standard bill_date format (Error 3)                        : {len(err_rows[3])}")
print(f"Rows with CASH-SALE retailer_id (Error 4)                                : {len(err_rows[4])}")
print(f"Rows where bill_value_inr ≠ delivered_qty × price excl. 11/12/13 (Err 5): {len(err_rows[5])}")
print(f"Rows where returned_qty > delivered_qty (must equal Error-6 + Error-12)   : {ret_gt_del}  (expected: {len(err_rows[6]) + len(err_rows[12])})")
print(f"Exact duplicate rows appended (Error 7)                                  : 250")
print(f"Partial duplicate rows appended (Error 8)                                : {len(err_rows[8])}")
print(f"New-Year date typo rows — Jan 2023 instead of Jan 2024 (Error 9)        : {len(err_rows[9])}")
print(f"UOM outlier rows — qty multiplied by carton factor (Error 10)            : {len(err_rows[10])}")
print(f"Free goods rows — delivered_qty > 0 and bill_value_inr = 0 (Error 11)   : {len(err_rows[11])}")
print(f"Negative delivered_qty rows — POS hack (Error 12)                        : {len(err_rows[12])}")
print(f"Orphaned sku_id rows — SKU-PROMO-01 or MISC-ITEM (Error 13)             : {len(err_rows[13])}")
print(f"Epoch or future bill_date rows (Error 14)                                : {len(err_rows[14])}")

print("\n" + "=" * 65)
print("Generation complete!")
