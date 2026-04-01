import pandas as pd
import numpy as np
import random
import os
import math
from datetime import date, timedelta
import calendar

# ==========================================
# 1. CONFIGURATION & CONSTANTS
# ==========================================
INPUT_DIR = r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data"
OUTPUT_FILE = r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data\fact_primary_sales.csv"

# Financial Targets
TARGETS = {
    "FY2023-24": {"min": 9520000, "max": 9775000},
    "FY2024-25": {"min": 22680000, "max": 23100000},
    "FY2025-26": {"min": 48300000, "max": 49220000}
}

# Base invoice values used as a starting point before scaling
BASE_INVOICE_VALUE = {"A": 55000, "B": 45000, "C": 35000}

# SKU Mix Ratios
# Spices relative volume mix (normalised to 1.0)
SPICE_MIX_FY24 = {
    "SPC-TUR-0200G": 0.175, "SPC-RCP-0200G": 0.175,
    "SPC-TUR-0100G": 0.10, "SPC-RCP-0100G": 0.10,
    "SPC-COR-0200G": 0.08, "SPC-COR-0100G": 0.07,
    "SPC-PNP-0100G": 0.08, "SPC-PNP-0050G": 0.07,
    "SPC-KKM-0100G": 0.08, "SPC-KKM-0050G": 0.07
}

SPICE_MIX_FY25_ONWARDS = SPICE_MIX_FY24.copy()
SPICE_MIX_FY25_ONWARDS["SPC-KKM-0100G"] = 0.055 # Adjusted to make room for PPF
SPICE_MIX_FY25_ONWARDS["SPC-KKM-0050G"] = 0.045
SPICE_MIX_FY25_ONWARDS["SPC-PPF-0050G"] = 0.05

SWEET_FLOUR_MIX = {
    "SWT-JGB-0500G": 0.25,
    "SWT-JGP-0500G": 0.20,
    "FLR-BSN-0500G": 0.30,
    "FLR-BSN-1000G": 0.25
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def get_fy(dt):
    if dt.month >= 4:
        return f"FY{dt.year}-{str(dt.year+1)[-2:]}"
    else:
        return f"FY{dt.year-1}-{str(dt.year)[-2:]}"

def get_quarter(dt):
    if dt.month in [4, 5, 6]: return "Q1"
    if dt.month in [7, 8, 9]: return "Q2"
    if dt.month in [10, 11, 12]: return "Q3"
    return "Q4"


def get_working_days(year, month):
    num_days = calendar.monthrange(year, month)[1]
    days = [date(year, month, day) for day in range(1, num_days + 1)]
    return [d for d in days if d.weekday() < 5] # Monday to Friday

def is_monsoon_assam(dt):
   return dt.year == 2025 and dt.month in [6, 7, 8, 9]

# ==========================================
# 3. LOAD DIMENSIONS & METADATA
# ==========================================
# We hardcode the distributor logic from the prompt to guarantee accuracy 
# regardless of slight schema variations in the input CSVs.
dist_meta = {}
# FY24 WB
for i, tier in zip(range(1, 12), ['A','A','B','A','B','B','B','A','A','A','A']):
    dist_meta[f"DIST-{i:03d}"] = {'state': 'WB', 'tier': tier, 'launch_fy': 'FY2023-24'}
# FY25 WB & OD
for i, tier in zip(range(12, 23), ['A','B','B','B','C','B','B','B','B','C','B']):
    state = 'OD' if i >= 20 else 'WB'
    dist_meta[f"DIST-{i:03d}"] = {'state': state, 'tier': tier, 'launch_fy': 'FY2024-25'}
# FY26 WB, OD, AS
for i, tier in zip(range(23, 37), ['C','A','B','A','B','A','B','B','B','A','B','A','C','C']):
    if i <= 25: state = 'WB'
    elif i <= 30: state = 'OD'
    else: state = 'AS'
    dist_meta[f"DIST-{i:03d}"] = {'state': state, 'tier': tier, 'launch_fy': 'FY2025-26'}

# Read SKU dim to get prices, cartons, launch FY
try:
    df_sku = pd.read_csv(os.path.join(INPUT_DIR, "dim_sku.csv"))
    sku_dict = df_sku.set_index('sku_id').to_dict('index')
except FileNotFoundError:
    raise FileNotFoundError(f"Ensure dim_sku.csv exists at {INPUT_DIR}")

# ==========================================
# 4. DISPATCH FREQUENCY GENERATION
# ==========================================
def get_dispatches(dist_id, tier, state, launch_fy, curr_fy, qtr):
    if curr_fy < launch_fy: return 0
    
    # Assam Override
    if state == 'AS' and curr_fy == 'FY2025-26':
        return 1
        
    year_of_op = int(curr_fy[2:6]) - int(launch_fy[2:6]) + 1
    
    if tier == 'C': return 1
    
    if tier == 'A':
        if year_of_op == 1:
            return 1 if qtr in ['Q1', 'Q2'] else 2
        else:
            return 3 if qtr == 'Q3' else 2
            
    if tier == 'B':
        if year_of_op == 1:
            return 1
        else:
            return 1 if qtr in ['Q1', 'Q2'] else 2

# ==========================================
# 5. DATA GENERATION ENGINE
# ==========================================
print("Generating raw invoice events...")
events = []
global_inv_counter = 1

start_date = date(2023, 4, 1)
end_date = date(2026, 3, 31)

# Generate Invoice Events Calendar
curr_date = start_date
while curr_date <= end_date:
    curr_fy = get_fy(curr_date)
    curr_qtr = get_quarter(curr_date)
    working_days = get_working_days(curr_date.year, curr_date.month)
    
    for dist_id, meta in dist_meta.items():
        if curr_fy >= meta['launch_fy']:
            num_dispatches = get_dispatches(dist_id, meta['tier'], meta['state'], meta['launch_fy'], curr_fy, curr_qtr)
            
            if num_dispatches > 0:
                dispatch_dates = sorted(random.sample(working_days, num_dispatches))
                for d_date in dispatch_dates:
                    events.append({
                        'invoice_id': f"INV-{global_inv_counter:05d}",
                        'invoice_date': d_date,
                        'distributor_id': dist_id,
                        'fy': curr_fy
                    })
                    global_inv_counter += 1
                    
    # Move to next month
    if curr_date.month == 12: curr_date = date(curr_date.year + 1, 1, 1)
    else: curr_date = date(curr_date.year, curr_date.month + 1, 1)

print(f"Total trucks dispatched: {len(events)}. Generating SKU line items...")
rows = []

for ev in events:
    dist_id = ev['distributor_id']
    inv_date = ev['invoice_date']
    fy = ev['fy']
    meta = dist_meta[dist_id]
    
    # Base Value Determination & Anomaly 1
    base_val = BASE_INVOICE_VALUE[meta['tier']]
    if dist_id == 'DIST-010':
        if fy == 'FY2024-25': base_val *= 0.89
        elif fy == 'FY2025-26': base_val *= 0.79
        
    # Categories split
    val_oil = base_val * (0.68 if fy == 'FY2023-24' else 0.62)
    val_spice = base_val * (0.32 if fy == 'FY2023-24' else 0.28)
    val_sweet = base_val * (0.0 if fy == 'FY2023-24' else 0.10)
    
    # Available SKUs
    active_skus = {k: v for k, v in sku_dict.items() if v['launch_fy'] <= fy}
    
    # OILS
    oil_prices = [active_skus['OIL-MUS-1000M']['distributor_price'], 
                  active_skus['OIL-MUS-0500M']['distributor_price'], 
                  active_skus['OIL-MUS-5000M']['distributor_price']]
    oil_weights = [0.5, 0.3, 0.2]
    avg_oil_price = sum(p*w for p,w in zip(oil_prices, oil_weights))
    total_oil_units = val_oil / avg_oil_price
    
    oil_alloc = {
        'OIL-MUS-1000M': total_oil_units * 0.5,
        'OIL-MUS-0500M': total_oil_units * 0.3,
        'OIL-MUS-5000M': total_oil_units * 0.2
    }
    
    # SPICES & Anomaly 2
    spice_mix = SPICE_MIX_FY24.copy() if fy == 'FY2023-24' else SPICE_MIX_FY25_ONWARDS.copy()
    
    failed_skus = ['SPC-PPF-0050G', 'SPC-KKM-0050G', 'SPC-KKM-0100G']
    if dist_id in ['DIST-020', 'DIST-021', 'DIST-022'] and fy != 'FY2023-24':
        if inv_date >= date(2024, 7, 1):
            for fs in failed_skus:
                if fs in spice_mix: del spice_mix[fs]
            # Renormalize remaining
            tot = sum(spice_mix.values())
            spice_mix = {k: v/tot for k, v in spice_mix.items()}
            
    avg_spice_price = sum(active_skus[s]['distributor_price'] * w for s, w in spice_mix.items() if s in active_skus)
    total_spice_units = val_spice / avg_spice_price if avg_spice_price > 0 else 0
    
    spice_alloc = {}
    for s, w in spice_mix.items():
        if s in active_skus:
            units = total_spice_units * w
            # June 24 reduction for Anomaly 2
            if dist_id in ['DIST-020', 'DIST-021', 'DIST-022'] and s in failed_skus and inv_date.year == 2024 and inv_date.month == 6:
                units *= 0.4 
            spice_alloc[s] = units

    # SWEETENERS & FLOURS
    sweet_alloc = {}
    if fy != 'FY2023-24':
        for s, w_val in SWEET_FLOUR_MIX.items():
            sweet_alloc[s] = (val_sweet * w_val) / active_skus[s]['distributor_price']
            
    # Combine and convert to carton multiples
    all_alloc = {**oil_alloc, **spice_alloc, **sweet_alloc}
    
    # Transit Lag
    if meta['state'] == 'WB': lag = random.randint(1, 3)
    elif meta['state'] == 'OD': lag = random.randint(2, 5)
    else: # AS
        lag = random.randint(8, 14) if is_monsoon_assam(inv_date) else random.randint(4, 7)
        
    grn = inv_date + timedelta(days=lag)
    if grn.weekday() == 6: grn += timedelta(days=1) # No Sundays
    
    for sku, raw_units in all_alloc.items():
        if raw_units <= 0: continue
        
        upc = active_skus[sku]['units_per_carton']
        price = active_skus[sku]['distributor_price']
        
        cartons = max(1, round(raw_units / upc))
        disp_qty = cartons * upc
        
        # Received Qty calculation (Anomaly 3 handled here implicitly)
        damage_factor = random.uniform(0.93, 0.97) if (meta['state'] == 'AS' and is_monsoon_assam(inv_date)) else random.uniform(0.99, 1.00)
        rec_qty = math.floor(disp_qty * damage_factor)
        
        rows.append({
            'invoice_id': ev['invoice_id'],
            'invoice_date': inv_date,
            'grn_date': grn,
            'distributor_id': dist_id,
            'sku_id': sku,
            'dispatched_qty': disp_qty,
            'received_qty': rec_qty,
            'invoice_value_inr': disp_qty * price,
            'fy': fy,
            'units_per_carton': upc,
            'price': price,
            'state': meta['state']
        })

df = pd.DataFrame(rows)

# ==========================================
# 6. FINANCIAL TARGET SCALING LOOP
# ==========================================
print("Scaling to meet exact financial targets...")

for fy_target, bounds in TARGETS.items():
    max_iter = 50
    for _iter in range(max_iter):
        current_sum = df[df['fy'] == fy_target]['invoice_value_inr'].sum()
        if bounds['min'] <= current_sum <= bounds['max']:
            break # Target met
            
        target_mid = (bounds['min'] + bounds['max']) / 2
        factor = target_mid / current_sum
        
        # Apply scaling keeping carton constraints
        mask = df['fy'] == fy_target
        
        # Recalculate dispatches
        new_raw = df.loc[mask, 'dispatched_qty'] * factor
        cartons = (new_raw / df.loc[mask, 'units_per_carton']).round()
        cartons = cartons.clip(lower=1)
        
        df.loc[mask, 'dispatched_qty'] = (cartons * df.loc[mask, 'units_per_carton']).astype(int)
        df.loc[mask, 'invoice_value_inr'] = (df.loc[mask, 'dispatched_qty'] * df.loc[mask, 'price']).astype(int)
        
        # Recalculate Received Qty
        is_monsoon = (df['state'] == 'AS') & (pd.to_datetime(df['invoice_date']).dt.year == 2025) & (pd.to_datetime(df['invoice_date']).dt.month.isin([6, 7, 8, 9]))
        
        # Apply random damage factors inline based on condition
        normal_mask = mask & ~is_monsoon
        monsoon_mask = mask & is_monsoon
        
        if normal_mask.any():
            random_factors = np.random.uniform(0.99, 1.00, normal_mask.sum())
            df.loc[normal_mask, 'received_qty'] = np.floor(df.loc[normal_mask, 'dispatched_qty'] * random_factors).astype(int)
            
        if monsoon_mask.any():
            random_factors_m = np.random.uniform(0.93, 0.97, monsoon_mask.sum())
            df.loc[monsoon_mask, 'received_qty'] = np.floor(df.loc[monsoon_mask, 'dispatched_qty'] * random_factors_m).astype(int)

# ==========================================
# 7. FINAL FORMATTING & KEYS
# ==========================================
# Sort as requested
df = df.sort_values(by=['invoice_date', 'distributor_id', 'sku_id']).reset_index(drop=True)

# Generate primary_sale_id
df['primary_sale_id'] = [f"PRI-{i:06d}" for i in range(100001, 100001 + len(df))]

# Clean columns
df['invoice_date'] = df['invoice_date'].astype(str)
df['grn_date'] = df['grn_date'].astype(str)

final_cols = ['primary_sale_id', 'invoice_id', 'invoice_date', 'grn_date', 
              'distributor_id', 'sku_id', 'dispatched_qty', 'received_qty', 'invoice_value_inr']

df_final = df[final_cols]

# ==========================================
# 8. VALIDATION CHECKS
# ==========================================
print("\n--- RUNNING VALIDATION CHECKS ---")

print(f"1. Total rows generated: {len(df_final)}")

print("2. Year-wise invoice_value_inr totals:")
fy_sums = df.groupby('fy')['invoice_value_inr'].sum()
for fy, val in fy_sums.items():
    status = "PASS" if TARGETS[fy]['min'] <= val <= TARGETS[fy]['max'] else "FAIL"
    print(f"   {fy}: ₹{val:,.0f} [{status}]")

assert df_final['primary_sale_id'].is_unique, "3. Duplicate primary_sale_id found"
print("3. No duplicate primary_sale_id - PASS")

assert not df_final.duplicated(subset=['invoice_id', 'sku_id']).any(), "4. (invoice_id, sku_id) duplicates found"
print("4. No (invoice_id, sku_id) duplicates - PASS")

assert (df['dispatched_qty'] % df['units_per_carton'] == 0).all(), "5. dispatched_qty not a multiple of units_per_carton"
print("5. dispatched_qty is always a multiple of units_per_carton - PASS")

assert (df_final['received_qty'] <= df_final['dispatched_qty']).all(), "6. received_qty > dispatched_qty found"
print("6. received_qty <= dispatched_qty for all rows - PASS")

inv_dates_dt = pd.to_datetime(df_final['invoice_date'])
assert not inv_dates_dt.dt.weekday.isin([5, 6]).any(), "7. invoice_date on weekend found"
print("7. No invoice_date on a Saturday or Sunday - PASS")

grn_dates_dt = pd.to_datetime(df_final['grn_date'])
assert not (grn_dates_dt.dt.weekday == 6).any(), "8. grn_date on Sunday found"
print("8. No grn_date on a Sunday - PASS")

assert (grn_dates_dt >= inv_dates_dt).all(), "9. grn_date < invoice_date found"
print("9. grn_date >= invoice_date for all rows - PASS")

# Assam monsoon check
assam_monsoon_mask = (df['state'] == 'AS') & (inv_dates_dt.dt.year == 2025) & (inv_dates_dt.dt.month.isin([6, 7, 8, 9]))
if assam_monsoon_mask.any():
    lags = (grn_dates_dt[assam_monsoon_mask] - inv_dates_dt[assam_monsoon_mask]).dt.days
    assert lags.between(8, 14).all(), "10. Assam monsoon transit lags not in 8-14 days"
print("10. Assam monsoon transit lags validated - PASS")

# Odisha anomaly check
odisha_failed_mask = (df_final['distributor_id'].isin(['DIST-020', 'DIST-021', 'DIST-022'])) & \
                     (df_final['sku_id'].isin(['SPC-PPF-0050G', 'SPC-KKM-0050G', 'SPC-KKM-0100G'])) & \
                     (inv_dates_dt >= pd.to_datetime('2024-07-01'))
assert odisha_failed_mask.sum() == 0, "11. Odisha failed SKUs dispatched after July 2024"
print("11. Odisha failed SKUs correctly removed after July 2024 - PASS")

# Launch FY check
sku_launch_fys = df_final['sku_id'].map(lambda x: sku_dict[x]['launch_fy'])
assert (df['fy'] >= sku_launch_fys).all(), "12. SKU used before its launch_fy"
print("12. No SKU used before its launch_fy - PASS")

# ==========================================
# 9. OUTPUT
# ==========================================
df_final.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
print(f"\nfact_primary_sales.csv successfully saved. Total rows: {len(df_final)}")

