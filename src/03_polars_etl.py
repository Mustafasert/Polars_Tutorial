import os
import time
import polars as pl

# Silence Arrow extension warning deterministically
os.environ.setdefault("POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR", "load_as_storage")

CONNECTION_URI = "postgresql://user:password@db:5432/etl_db"
QUERY = "SELECT category, region, amount, sale_date FROM sales"
OUTPUT_PATH = "/app/data/electronics_regional_summary.csv"

def run_etl_pipeline():
    print(">>> STARTING POLARS ETL PIPELINE <<<")

    t_total0 = time.perf_counter()

    # 1) EXTRACTION
    print("1. Extracting data from PostgreSQL...")
    t0 = time.perf_counter()
    df = pl.read_database_uri(query=QUERY, uri=CONNECTION_URI, engine="adbc")
    t_extract = time.perf_counter() - t0
    print(f"Extracted {df.height} rows from the database.")
    print("Schema from DB:")
    print(df.schema)
    print(f"Data extraction completed in {t_extract:.2f} seconds.")

    # 2) CLEANING (amount -> numeric)
    print("\n2. Cleaning & transforming data...")
    t0 = time.perf_counter()
    print(df.dtypes)
    df = df.with_columns(pl.col("amount").cast(pl.Float64, strict=False))
    t_clean = time.perf_counter() - t0
    print(f"Data cleaning completed in {t_clean:.2f} seconds.")

    # 3) AGGREGATION
    print("\n3. Aggregating (Electronics by region)...")
    t0 = time.perf_counter()
    transformed_df = (
        df.filter(pl.col("category") == "Electronics")
          .group_by("region")
          .agg(
              pl.col("amount").sum().round(2).alias("total_sales_revenue"),
              pl.col("amount").mean().round(2).alias("average_sale_value"),
              pl.len().alias("total_transactions"),
          )
          .sort("total_sales_revenue", descending=True)
    )
    t_agg = time.perf_counter() - t0
    print("Aggregation complete. Preview:")
    print(transformed_df)
    print(f"Aggregation completed in {t_agg:.2f} seconds.")

    # 4) LOAD (write CSV)
    print(f"\n4. Writing results to {OUTPUT_PATH}...")
    t0 = time.perf_counter()
    transformed_df.write_csv(OUTPUT_PATH)
    t_write = time.perf_counter() - t0
    print(f"CSV write completed in {t_write:.2f} seconds.")

    t_total = time.perf_counter() - t_total0
    print(
        "\n[SUCCESS] Polars ETL completed.\n"
        f"Timings (s): extract={t_extract:.2f}, clean={t_clean:.2f}, "
        f"agg={t_agg:.2f}, write={t_write:.2f}, total={t_total:.2f}"
    )

if __name__ == "__main__":
    run_etl_pipeline()