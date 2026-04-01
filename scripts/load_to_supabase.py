import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

# ── Load environment variables from .env ─────────────────────────────────────
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# ── Data directory ────────────────────────────────────────────────────────────
DATA_DIR = Path(r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data")

# ── Load order: dimensions first, then facts ──────────────────────────────────
TABLES = [
    "dim_territory",
    "dim_sku",
    "dim_distributor",
    "dim_retailer",
    "fact_target",
    "fact_primary_sales",
    "fact_secondary_sales",
]

def main():
    print("Connecting to Supabase...")
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✓ Connection successful\n")

    for table in TABLES:
        csv_path = DATA_DIR / f"{table}.csv"

        if not csv_path.exists():
            print(f"✗ Skipped — file not found: {csv_path}")
            continue

        print(f"Loading {table} ...", end=" ", flush=True)

        df = pd.read_csv(csv_path, dtype=str)

        df.to_sql(
            name=f"raw_{table}",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=10_000,
            method="multi",
        )

        print(f"✓  {len(df):,} rows  →  raw_{table}")

    print("\n" + "=" * 55)
    print("All tables loaded successfully into Supabase.")
    print("Next step: open DBeaver and run your CTAS scripts.")

if __name__ == "__main__":
    main()