-- Databricks notebook source
--Top 10 restaurants by Revenue
--Top 10 restaurants by orders
--Revenue distribution among top 10 restaurants


WITH orders AS
(
    SELECT restaurant_id, total_orders, rn_ord
    FROM
    (
        SELECT restaurant_id, total_orders,
        ROW_NUMBER() OVER (ORDER BY total_orders DESC) AS rn_ord 
        FROM project_1.gold.gold_restaurant_performance
    )
    WHERE rn_ord <= 10
),
revenue AS
(
    SELECT restaurant_id, total_revenue, rn_rev
    FROM
    (
        SELECT restaurant_id, total_revenue,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS rn_rev
        FROM project_1.gold.gold_restaurant_performance
    )
    WHERE rn_rev <= 10
)
SELECT o.restaurant_id AS order_res_id, o.total_orders AS total_orders, r.restaurant_id AS revenue_res_id, r.total_revenue AS total_revenue
FROM orders o
JOIN revenue r
ON o.rn_ord = r.rn_rev
;


-- COMMAND ----------

--Top 10 restaurants by completion rate

SELECT restaurant_id, ROUND(completed_orders*100/total_orders,2) AS completion_rate 
FROM project_1.gold.gold_restaurant_performance
WHERE total_orders > 0
ORDER BY completion_rate DESC
LIMIT 10;