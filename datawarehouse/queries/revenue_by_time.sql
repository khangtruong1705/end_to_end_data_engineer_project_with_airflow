-- File: queries/02_revenue_by_time.sql
-- Mô tả: Doanh thu theo năm-tháng (dùng cho line chart)

SELECT
    d.year,
    d.month,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2) AS revenue
FROM fact_order f
JOIN dim_date d ON f.purchase_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year;