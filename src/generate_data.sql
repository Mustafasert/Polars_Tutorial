-- Create the sales table schema
-- We do not insert data here; data will be bulk loaded via Python.
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    category VARCHAR(50),
    region VARCHAR(50),
    amount NUMERIC,
    sale_date DATE
);