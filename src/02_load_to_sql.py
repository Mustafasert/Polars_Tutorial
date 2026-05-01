import polars as pl
import time

FILE_PATH = "/app/data/large_sales_data.csv"
CONNECTION_URI = "postgresql://user:password@db:5432/etl_db"
TABLE_NAME = "public.sales"

def load_csv_to_postgres():
    print(f"Reading data from {FILE_PATH}...")
    start_time = time.time()

    # 1. GEREKSİZ OVERRIDE KALDIRILDI: 
    # Polars zaten sayısal olan 'amount'u otomatik tanıyacaktır.
    df = pl.read_csv(FILE_PATH)

    # 2. GEREKSİZ SÜTUNLARI ATMA
    if "id" in df.columns:
        df = df.drop("id")

    # 3. SADELEŞTİRİLMİŞ DÖNÜŞÜMLER
    df = df.with_columns([
        # Sadece tarih dönüşümü yeterli (CSV'den string gelir)
        pl.col("sale_date").str.to_date("%Y-%m-%d"),
        
        # Karmaşık regex temizliğine gerek yok, direkt cast et
        # Eğer amount zaten numeric okunmuşsa bu adım hata vermez, pas geçer.
        pl.col("amount").cast(pl.Decimal(15, 2))
    ])

    print(f"Schema confirmed: {df.schema}")
    print(f"Loaded {df.height} rows. Writing to PostgreSQL...")

    # 4. YAZMA İŞLEMİ
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