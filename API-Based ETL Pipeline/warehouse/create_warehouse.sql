-- Create a separate schema for better organization
CREATE SCHEMA IF NOT EXISTS warehouse;
-- Dimension Tables
CREATE TABLE IF NOT EXISTS warehouse.dim_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE
);
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category_id INT REFERENCES warehouse.dim_category(category_id),
    brand_name VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    month INT,
    year INT,
    weekday VARCHAR(20)
);
-- Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES warehouse.dim_product(product_id),
    quantity INT,
    total_amount DECIMAL(10, 2),
    sale_date DATE REFERENCES warehouse.dim_date(date)
);