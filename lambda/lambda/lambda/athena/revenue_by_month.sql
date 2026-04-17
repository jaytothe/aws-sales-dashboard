SELECT 
  SUBSTRING(date, 1, 7) AS month,
  ROUND(SUM(amount), 2) AS total_revenue
FROM sales_dashboard.processed
GROUP BY SUBSTRING(date, 1, 7)
ORDER BY month;
