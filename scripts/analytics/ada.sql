-- Change over time analysis
SELECT 
    EXTRACT (YEAR FROM order_date) AS order_year,
    EXTRACT (MONTH FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

-- Cumulative analysis
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
    AVG(avg_price) OVER (ORDER BY order_date) AS moving_avg_price
FROM (
    SELECT 
        date_trunc('year', order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY date_trunc('year', order_date)
) t;

-- Performance analysis
WITH yearly_products_sales AS (
SELECT 
    EXTRACT (YEAR FROM f.order_date) AS order_year,
    p.product_name,
    SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY order_year, p.product_name
)
SELECT 
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg
FROM yearly_products_sales
ORDER BY product_name, order_year;

-- Part-to-whole
WITH category_sales AS (
SELECT
    category,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p 
ON p.product_key = f.product_key
GROUP BY category
)
SELECT 
    category,
    total_sales,
    SUM(total_sales) OVER () overall_sales,
    CONCAT(ROUND((total_sales / SUM(total_sales) OVER ())*100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;

-- Data Segmentation
WITH product_segments AS (
SELECT
    product_key,
    product_name,
    cost,
    CASE WHEN cost < 100 THEN 'Bellow 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-100'
        ELSE 'Above 1000'
    END cost_range
FROM gold.dim_products
)
SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range 
ORDER BY total_products DESC;