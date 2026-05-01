import polars as pl
import time

FILE_PATH = "/app/data/large_sales_data.csv"
CONNECTION_URI = "postgresql://user:password@db:5432/etl_db"
TABLE_NAME = "public.sales"

def load_csv_to_postgres():
    print(f"Reading data from {FILE_PATH}...")
    start_time = time.time()

    # 1. READ CSV with Polars
    df = pl.read_csv(FILE_PATH)

    # 2 remove "id" column if it exists, as it's not needed for the database
    if "id" in df.columns:
        df = df.drop("id")

    # 3. Data conversion
    df = df.with_columns([
        pl.col("sale_date").str.to_date("%Y-%m-%d"),
        pl.col("amount").cast(pl.Decimal(15, 2))
    ])

    print(f"Schema confirmed: {df.schema}")
    print(f"Loaded {df.height} rows. Writing to PostgreSQL...")

    # 4. WRITE to PostgreSQL using ADBC
    df.write_database(
        table_name=TABLE_NAME,
        connection=CONNECTION_URI,
        if_table_exists="append",
        engine="adbc",
    )

    end_time = time.time()
    print(f"Done! Time taken: {end_time - start_time:.2f} seconds\n")

if __name__ == "__main__":
    load_csv_to_postgres()