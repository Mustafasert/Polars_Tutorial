import polars as pl
import pandas as pd
import numpy as np
import time
import tracemalloc
import os

FILE_PATH = "/app/data/large_sales_data.csv"
NUM_ROWS = 5_000_000

def generate_data():
    """5 Milyon satırlık test verisi oluşturur."""
    if not os.path.exists(FILE_PATH):
        print(f"[{NUM_ROWS} satırlık dev veri seti oluşturuluyor. Lütfen bekleyin...]")
        # Veriyi hızlı üretmek için yine Polars kullanıyoruz
        df_gen = pl.DataFrame({
            "id": np.arange(NUM_ROWS),
            "category": np.random.choice(["Electronics", "Furniture", "Clothing", "Food", "Toys"], NUM_ROWS),
            "region": np.random.choice(["North", "South", "East", "West"], NUM_ROWS),
            "amount": np.random.rand(NUM_ROWS) * 1000
        })
        df_gen.write_csv(FILE_PATH)
        print("Veri seti başarıyla oluşturuldu!\n")
    else:
        print("Veri seti zaten mevcut, testlere geçiliyor...\n")

def test_pandas():
    print(">>> PANDAS TESTİ BAŞLIYOR <<<")
    tracemalloc.start() # RAM takibini başlat
    start_time = time.time()

    # Pandas verinin tamamını RAM'e yüklemek zorundadır (Eager execution)
    df = pd.read_csv(FILE_PATH)
    
    # 500'den büyük satışları filtrele, kategori ve bölgeye göre grupla, toplamı al
    result = df[df["amount"] > 500].groupby(["category", "region"])["amount"].sum()

    end_time = time.time()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    time_taken = end_time - start_time
    ram_used = peak / (1024 * 1024) # MB cinsine çevir
    print(f"Pandas Süre: {time_taken:.2f} saniye")
    print(f"Pandas Max RAM: {ram_used:.2f} MB\n")
    return time_taken, ram_used

def test_polars():
    print(">>> POLARS TESTİ BAŞLIYOR <<<")
    tracemalloc.start()
    start_time = time.time()

    # Polars 'Lazy Evaluation' (Tembel Değerlendirme) kullanır.
    # scan_csv sadece işlem planı çıkarır, collect() diyene kadar RAM'e yüklemez.
    result = (
        pl.scan_csv(FILE_PATH)
        .filter(pl.col("amount") > 500)
        .group_by(["category", "region"])
        .agg(pl.col("amount").sum())
        .collect() # İşlemi burada gerçekleştirir (Query Optimizer devreye girer)
    )

    end_time = time.time()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    time_taken = end_time - start_time
    ram_used = peak / (1024 * 1024)
    print(f"Polars Süre: {time_taken:.2f} saniye")
    print(f"Polars Max RAM: {ram_used:.2f} MB\n")
    return time_taken, ram_used

if __name__ == "__main__":
    generate_data()
    pd_time, pd_ram = test_pandas()
    pl_time, pl_ram = test_polars()
    
    print("-" * 40)
    print("ÖZET KARŞILAŞTIRMA:")
    print(f"Hız Farkı: Polars, Pandas'tan {pd_time / pl_time:.1f} kat daha hızlı!")
    print(f"RAM Tasarrufu: Polars, Pandas'tan {pd_ram / pl_ram:.1f} kat daha az RAM kullandı!")
    print("-" * 40)