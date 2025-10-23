-- Total sales by category
SELECT c.category_name,
    SUM(f.total_amount) AS total_revenue
FROM warehouse.fact_sales f
    JOIN warehouse.dim_product p ON f.product_id = p.product_id
    JOIN warehouse.dim_category c ON p.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_revenue DESC;
-- Top 5 best-selling products
SELECT p.product_name,
    SUM(f.quantity) AS total_quantity
FROM warehouse.fact_sales f
    JOIN warehouse.dim_product p ON f.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_quantity DESC
LIMIT 5;
-- Monthly revenue trend
SELECT d.year,
    d.month,
    SUM(f.total_amount) AS monthly_sales
FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON f.sale_date = d.date
GROUP BY d.year,
    d.month
ORDER BY d.year,
    d.month;
-- Average product price by category
SELECT c.category_name,
    AVG(p.brand_name IS NULL) AS avg_price
FROM warehouse.dim_product p
    JOIN warehouse.dim_category c ON p.category_id = c.category_id
GROUP BY c.category_name;