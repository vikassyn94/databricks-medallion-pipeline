-- Databricks notebook source
-- Yesterday vs Last 7 days metrics

SELECT order_date, net_revenue, total_orders, avg_order_value, completed_orders, failed_orders,
AVG(total_orders) OVER(ORDER BY order_date RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW) AS rolling_7_days_avg_orders,
AVG(net_revenue) OVER(ORDER BY order_date RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW) AS rolling_7_days_avg_revenue,
AVG(avg_order_value) OVER(ORDER BY order_date RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW) AS rolling_7_days_aov,
AVG(completed_orders) OVER(ORDER BY order_date RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW) AS rolling_7_days_completed_orders,
AVG(failed_orders) OVER(ORDER BY order_date RANGE BETWEEN INTERVAL '6 DAYS' PRECEDING AND CURRENT ROW) AS rolling_7_days_failed_orders
FROM project_1.gold.gold_daily_business_summary
ORDER BY order_date DESC;

-- COMMAND ----------

--KPI metrics
--DOD revenue trend
--DOD orders trend
--DOD aov trend
--DOD orders distribution trend

SELECT * 
FROM project_1.gold.gold_daily_business_summary;

-- COMMAND ----------

--DOD customer count trend

SELECT * FROM
project_1.gold.gold_customer_activity;