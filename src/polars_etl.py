import polars as pl

# 1. Ingestion (PostgreSQL'den okuma)
connection_uri = "postgresql://user:password@db:5432/etl_db"
query = "SELECT * FROM sales"

print("PostgreSQL'den veri çekiliyor...")
# Polars read_database fonksiyonu ile veriyi doğrudan DataFrame'e çeker
df = pl.read_database(query, connection_uri)
print("Orijinal Veri:")
print(df)

# 2. Transformation (Polars ile dönüşüm)
print("\nPolars ile veri işleniyor (ETL Transformation)...")
# Sadece "Electronics" kategorisini filtrele ve toplam satış tutarını hesapla
transformed_df = (
    df.filter(pl.col("category") == "Electronics")
      .group_by("category")
      .agg(
          pl.col("amount").sum().alias("total_sales"),
          pl.col("product_name").count().alias("items_sold")
      )
)
print("Dönüştürülmüş Veri:")
print(transformed_df)

# 3. Load / Dışa Aktarım
output_path = "/app/data/electronics_summary.csv"
transformed_df.write_csv(output_path)
print(f"\nİşlem tamamlandı. Sonuçlar '{output_path}' konumuna kaydedildi.")