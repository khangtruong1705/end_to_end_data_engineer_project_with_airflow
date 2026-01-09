-- File: queries/07_delivery_performance.sql
-- Mô tả: Tỷ lệ giao đúng hạn theo tháng

SELECT
    d.year,
    d.month,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN f.is_late_delivery THEN 1 ELSE 0 END) AS late_orders,
    ROUND(SUM(CASE WHEN NOT f.is_late_delivery THEN 1 ELSE 0 END)::NUMERIC / COUNT(*) * 100, 2) AS on_time_delivery_rate
FROM fact_order f
JOIN dim_date d ON f.delivery_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;