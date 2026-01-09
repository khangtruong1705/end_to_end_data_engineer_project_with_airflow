-- File: queries/01_kpi_overview.sql
-- Mô tả: Các KPI chính cho dashboard overview

SELECT
    ROUND(SUM(price + freight_value)::NUMERIC, 2) AS total_revenue,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    ROUND((SUM(price + freight_value) / COUNT(DISTINCT order_id))::NUMERIC,2) AS average_order_value,
    ROUND(AVG(review_score_avg)::NUMERIC, 2) AS average_review_score,
    ROUND(AVG(delivery_time_days)::NUMERIC, 1) AS average_delivery_time_days,
    ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END)::NUMERIC / COUNT(*),4) AS late_delivery_rate
FROM fact_order;