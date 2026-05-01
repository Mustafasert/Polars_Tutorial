import time
import pandas as pd
import sqlalchemy

CONNECTION_URI = "postgresql://user:password@db:5432/etl_db"
QUERY = "SELECT category, region, amount, sale_date FROM sales"
OUTPUT_PATH = "/app/data/electronics_regional_summary_pandas.csv"

def run_etl_pipeline():
    print(">>> STARTING PANDAS ETL PIPELINE <<<")

    t_total0 = time.perf_counter()

    # 1) EXTRACTION
    print("1. Extracting data from PostgreSQL...")
    t0 = time.perf_counter()
    engine = sqlalchemy.create_engine(CONNECTION_URI)
    df = pd.read_sql(QUERY, engine)
    t_extract = time.perf_counter() - t0
    print(f"Extracted {len(df)} rows from the database.")
    print(f"Data extraction completed in {t_extract:.2f} seconds.")

    # 2) CLEANING (amount -> numeric)
    print("\n2. Cleaning & transforming data...")
    t0 = time.perf_counter()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])
    t_clean = time.perf_counter() - t0
    print(f"Data cleaning completed in {t_clean:.2f} seconds.")

    # 3) AGGREGATION
    print("\n3. Aggregating (Electronics by region)...")
    t0 = time.perf_counter()
    transformed_df = (
        df[df["category"] == "Electronics"]
        .groupby("region", as_index=False)
        .agg(
            total_sales_revenue=("amount", lambda x: round(x.sum(), 2)),
            average_sale_value=("amount", lambda x: round(x.mean(), 2)),
            total_transactions=("amount", "size"),
        )
        .sort_values(by="total_sales_revenue", ascending=False)
    )
    t_agg = time.perf_counter() - t0
    print("Aggregation complete. Preview:")
    print(transformed_df)
    print(f"Aggregation completed in {t_agg:.2f} seconds.")

    # 4) LOAD (write CSV)
    print(f"\n4. Writing results to {OUTPUT_PATH}...")
    t0 = time.perf_counter()
    transformed_df.to_csv(OUTPUT_PATH, index=False)
    t_write = time.perf_counter() - t0
    print(f"CSV write completed in {t_write:.2f} seconds.")

    t_total = time.perf_counter() - t_total0
    print(
        "\n[SUCCESS] Pandas ETL completed.\n"
        f"Timings (s): extract={t_extract:.2f}, clean={t_clean:.2f}, "
        f"agg={t_agg:.2f}, write={t_write:.2f}, total={t_total:.2f}"
    )

if __name__ == "__main__":
    run_etl_pipeline()