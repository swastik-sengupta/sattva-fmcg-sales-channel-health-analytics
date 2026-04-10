import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

data_path = r"C:\Users\Admin\Desktop\SWASTIK\Sattva_FMCG_Sales_Analytics\data"

files = {
    "dim_distributor":     "dim_distributor.csv",
    "dim_retailer":        "dim_retailer.csv",
    "dim_sku":             "dim_sku.csv",
    "dim_territory":       "dim_territory.csv",
    "fact_primary_sales":  "fact_primary_sales.csv",
    "fact_secondary_sales":"fact_secondary_sales.csv",
    "fact_target":         "fact_target.csv",
}

for table_name, file_name in files.items():
    file_path = os.path.join(data_path, file_name)
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists="replace", index=False, schema="public")
    print(f"✓ Loaded {table_name} — {len(df)} rows")

print("\nAll tables loaded successfully.")