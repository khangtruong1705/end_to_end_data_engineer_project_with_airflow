-- File: queries/03_top_product_categories.sql
-- Mô tả: Top 10 danh mục theo doanh thu

SELECT
    p.product_category_name,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2) AS revenue,
    COUNT(*) AS items_sold,
    ROUND(AVG(f.review_score_avg)::NUMERIC, 2) AS avg_review_score
FROM fact_order f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_category_name
ORDER BY revenue DESC
LIMIT 10;