SELECT 
  rep,
  COUNT(order_id) AS total_orders,
  ROUND(SUM(amount), 2) AS total_revenue
FROM sales_dashboard.processed
GROUP BY rep
ORDER BY total_revenue DESC;
