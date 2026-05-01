## What is Polars and what are the advantages in data engineering pipelines?

**Polars** is a **DataFrame** library similar to Pandas, but it is built in **Rust**, so it is usually **faster** and more **memory-efficient**.  
In this project, we run the same ETL steps (Extract → Transform → Load) with both **Polars** and **Pandas** and compare the performance.

### Why Polars? (Key Advantages)

- **Faster (multi-threaded):** Polars can run many operations in parallel by default.
- **Lower RAM usage:** It uses a columnar (Apache Arrow-friendly) memory model, which reduces copying and overhead.
- **Great for ETL workloads:** Filtering, groupby aggregations, string cleaning, and date/numeric conversions are typically very efficient.
- **Lazy mode (optional):** With `LazyFrame`, Polars can optimize the query plan (read fewer columns, push filters earlier, etc.).

### Data Engineering pipeline in this repository

This repo contains 3 main phases:

1. **Generate (Data Generation):**  
   Creates 1,000,000 rows of synthetic sales data and writes it to a CSV file.

2. **Load (Database Loading):**  
   Cleans the CSV (especially `amount` and `sale_date`) and bulk loads it into PostgreSQL.

3. **ETL + Benchmark:**  
   Reads data from PostgreSQL, filters only the `Electronics` category, then groups by region to compute:
   - total revenue (`sum`)
   - average sale value (`mean`)
   - number of transactions (`count`)  
   Finally, it exports the result to CSV.  
   The same job is executed with **Polars** and **Pandas** so we can compare runtimes.

**Data Generation → Database Loading → ETL Transformation → Benchmarking**

---

## 1. What is this project?

You will:

- Generate **1,000,000** synthetic sales records (CSV)
- Clean & load the dataset into **PostgreSQL** using **Polars + ADBC**
- Run the same ETL aggregation with:
  - **Polars (ADBC)**
  - **Pandas (SQLAlchemy)**
- Export results to CSV and compare performance

---

## 2. Prerequisites

- **Docker** & **Docker Compose** (20.10+ recommended)

---

## 3. Build (Required)

Build the Python environment image first:

```bash
docker build -t polars-env:latest .
```

> This step is mandatory because `docker-compose.yml` uses the `polars-env:latest` image.

---

## 4. Environment Setup (Start Services)

Start the services (PostgreSQL + Python container):

```bash
docker-compose up -d
```

---

## 5. Execution Sequence (Pipeline)

### Phase 1: Data Generation

```bash
docker exec -it polars_python python src/01_generate_data.py
```

### Phase 2: Database Loading & Cleaning

```bash
docker exec -it polars_python python src/02_load_to_sql.py
```

### Phase 3: ETL Benchmark

#### Polars ETL

```bash
docker exec -it polars_python python src/03_polars_etl.py
```

#### Pandas ETL

```bash
docker exec -it polars_python python src/03_pandas_etl.py
```

---

## 6. Performance Comparison

### Sample timings (your run, 1,000,000 rows)

Measured on your machine from console output:

| Step | Polars | Pandas |
|------|--------|--------|
| DB extract | 0.83 s | 2.15 s |
| Amount cleaning | 0.16 s | 0.49 s |

> Note: Total runtime is now measured correctly using `time.perf_counter()` and per-step timers inside each script. Re-run the scripts after updating them to see accurate `total` timings.

---

## 7. Project Structure

```text
├── data/                       # Generated CSVs and ETL outputs
├── src/
│   ├── 01_generate_data.py     # Synthetic data generator
│   ├── 02_load_to_sql.py       # CSV → Postgres (cleaning included)
│   ├── 03_polars_etl.py        # Polars ETL implementation
│   └── 03_pandas_etl.py        # Pandas ETL implementation
├── docker-compose.yml
└── Dockerfile
```

---

## 8. Troubleshooting

### Warning: `Extension type 'arrow.opaque' is not registered`

If you use Polars + ADBC and see this warning, set:

- `POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR=load_as_storage`

in the `polars_python` service environment (docker-compose) to silence it.

---

## 9. Teardown

```bash
docker-compose down -v
```