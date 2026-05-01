# 1. Temel imajı belirle (Python'ın hafif sürümü)
FROM python:3.11-slim

# 2. Çalışma dizini oluştur
WORKDIR /app

# 3. PostgreSQL için gerekli sistem kütüphanelerini kur
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 4. Önce sadece gereksinim dosyasını kopyala (Önbellek avantajı için!)
COPY requirements.txt .

# 5. Bağımlılıkları kur
RUN pip install -r requirements.txt

# 6. Kaynak kodlarını 'src' klasörüne kopyala
# (Yerel bilgisayarınızdaki src klasörünü imajın içindeki /app/src'ye atar)
COPY src/ ./src/

# 7. Veri klasörünü oluştur (CSV dosyaları buraya gelecek)
RUN mkdir -p /app/data

# Konteyner çalışınca varsayılan olarak ne yapacağını belirtebiliriz 
# (Opsiyonel, biz genelde docker exec ile çalıştırıyoruz)
CMD ["python"]