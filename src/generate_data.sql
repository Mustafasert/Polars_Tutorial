CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(50),
    category VARCHAR(50),
    amount NUMERIC,
    sale_date DATE
);

INSERT INTO sales (product_name, category, amount, sale_date) VALUES
('Laptop', 'Electronics', 1200.50, '2026-04-01'),
('Mouse', 'Electronics', 25.00, '2026-04-02'),
('Desk', 'Furniture', 350.00, '2026-04-03'),
('Chair', 'Furniture', 150.00, '2026-04-04'),
('Monitor', 'Electronics', 300.00, '2026-04-05');