-- File: queries/04_revenue_by_state.sql
-- Mô tả: Doanh thu theo bang khách hàng (dùng cho Filled Map)

SELECT
    c.customer_state,
    ROUND(SUM(f.price + f.freight_value)::NUMERIC, 2) AS revenue,
    COUNT(DISTINCT f.order_id) AS orders_count
FROM fact_order f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_state
ORDER BY revenue DESC;