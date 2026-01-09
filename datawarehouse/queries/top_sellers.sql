-- File: queries/06_top_sellers.sql
-- Mô tả: Top 10 nhà bán theo doanh thu

SELECT
    s.seller_id,
    s.city AS seller_city,
    s.state AS seller_state,
    ROUND(SUM(f.price + f.freight_value), 2) AS revenue,
    COUNT(*) AS items_sold,
    ROUND(AVG(f.review_score_avg), 2) AS avg_review_score
FROM fact_orders f
JOIN dim_seller s ON f.seller_key = s.seller_key
GROUP BY s.seller_id, s.city, s.state
ORDER BY revenue DESC
LIMIT 10;