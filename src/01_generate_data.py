import polars as pl
import numpy as np
from datetime import datetime, timedelta
import os

FILE_PATH = "/app/data/large_sales_data.csv"
NUM_ROWS = 1_000_000 

def generate_csv_data():
    """Generates a large realistic dataset and saves it to a CSV file."""
    
    os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

    print(f"Generating synthetic dataset with {NUM_ROWS} rows. Please wait...")
    
    # Generate random dates for the last 365 days
    base_date = datetime(2025, 1, 1).date()
    random_days = np.random.randint(0, 365, NUM_ROWS)
    
    # (%Y-%m-%d)
    dates = [(base_date + timedelta(days=int(d))).strftime("%Y-%m-%d") for d in random_days]

    # Generate synthetic data
    df_gen = pl.DataFrame({
        "id": np.arange(1, NUM_ROWS + 1),
        "product_name": np.random.choice(["Laptop", "Mouse", "Desk", "Chair", "Monitor", "Keyboard", "Tablet"], NUM_ROWS),
        "category": np.random.choice(["Electronics", "Furniture", "Accessories"], NUM_ROWS),
        "region": np.random.choice(["North", "South", "East", "West"], NUM_ROWS),
        "amount": np.round(np.random.uniform(1.0, 250.0, NUM_ROWS), 2),
        "sale_date": dates
    })

    # Save to CSV
    df_gen.write_csv(FILE_PATH)
    print(f"Successfully generated and saved {NUM_ROWS} rows to {FILE_PATH}!\n")

if __name__ == "__main__":
    generate_csv_data()