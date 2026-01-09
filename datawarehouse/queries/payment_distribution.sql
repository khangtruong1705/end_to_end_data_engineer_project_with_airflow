-- File: queries/05_payment_distribution.sql
-- Mô tả: Doanh thu theo phương thức thanh toán

SELECT
    p.payment_type,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2) AS revenue,
    COUNT(DISTINCT f.order_id) AS orders_count,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC/ SUM(SUM(f.price + f.freight_value)) OVER ()::NUMERIC,4) * 100 AS revenue_percentage
FROM fact_order f
JOIN dim_payment p ON f.payment_key = p.payment_key
GROUP BY p.payment_type
ORDER BY revenue DESC;
