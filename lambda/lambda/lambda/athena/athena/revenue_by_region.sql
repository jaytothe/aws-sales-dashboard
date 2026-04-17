SELECT 
  region,
  ROUND(SUM(amount), 2) AS total_revenue
FROM sales_dashboard.processed
GROUP BY region
ORDER BY total_revenue DESC;
