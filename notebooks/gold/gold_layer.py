# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_orders = (
    spark.sql(f"""
              SELECT
                order_date AS order_date,
                COUNT(order_id) AS total_orders,
                SUM(amount)::double AS total_revenue
              FROM project_1.silver.orders
                GROUP BY order_date
              """)
)

df_orders \
.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("project_1.gold.gold_orders_summary")

# COMMAND ----------

'''
- Which restaurants are performing best?
- How much revenue each generates?
- What’s their order volume + AOV?
'''

df_restaurants = spark.sql("""
SELECT
    restaurant_id,
    COUNT(order_id) AS total_orders,
    COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed_orders,
    SUM(amount)::float AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value,
    MAX(order_date) AS last_order_date
FROM project_1.silver.orders
GROUP BY restaurant_id
""")


df_restaurants \
.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable("project_1.gold.gold_restaurant_performance")

# COMMAND ----------

'''
What items sell the most?
Which items generate the most revenue?
What’s driving orders?
'''

df_items = spark.sql("""
SELECT
    item_name,
    SUM(quantity) AS total_quantity_sold,
    SUM(quantity * price)::float AS total_revenue,
    ROUND(AVG(price), 2) AS avg_price
FROM project_1.silver.order_items
GROUP BY item_name
""")

df_items.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("project_1.gold.gold_item_performance")

# COMMAND ----------

df_summary = (
    spark.sql(f"""
            WITH orders AS
            (
                SELECT
                    order_date,
                    COUNT(*) AS total_orders,
                    COUNT(CASE WHEN order_status = 'COMPLETED' THEN 1 END) AS completed_orders,
                    COUNT(CASE WHEN order_status != 'COMPLETED' THEN 1 END) AS failed_orders,
                    SUM(amount)::float AS total_revenue,
                    SUM(CASE WHEN order_status = 'COMPLETED' THEN amount END)::float AS net_revenue,
                    AVG(CASE WHEN order_status = 'COMPLETED' THEN amount END)::float AS avg_order_value,
                    COUNT(DISTINCT restaurant_id) AS unique_restaurants
                FROM
                    project_1.silver.orders
                GROUP BY
                    order_date
            ),
            items AS 
            (
                SELECT 
                    o.order_date AS order_date,
                    SUM(i.quantity) AS total_items_sold
                FROM 
                    project_1.silver.orders o
                INNER JOIN 
                    project_1.silver.order_items i
                ON 
                    o.order_id = i.order_id
                GROUP BY
                    o.order_date
            )
            SELECT
                o.order_date,
                o.total_orders,
                o.completed_orders,
                o.failed_orders,
                o.total_revenue,
                o.net_revenue,
                o.avg_order_value,
                o.unique_restaurants,
                i.total_items_sold
            FROM
                orders o
            INNER JOIN
                items i
            ON
                o.order_date = i.order_date
              """)
)

df_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("project_1.gold.gold_daily_business_summary")

# COMMAND ----------

df_customer = (
    spark.sql(f"""
            WITH ranked_cx AS
            (
                SELECT
                    order_date,
                    customer_id,
                    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn
                FROM (
                    SELECT
                        order_date,
                        customer_id
                    FROM project_1.silver.orders
                    GROUP BY
                        order_date,
                        customer_id
                    )
            )
            SELECT
                order_date,
                COUNT(CASE WHEN rn = 1 THEN 1 END) AS new_customers,
                COUNT(CASE WHEN rn > 1 THEN 1 END) AS returning_customers,
                COUNT(DISTINCT customer_id) AS total_customers
            FROM ranked_cx
            GROUP BY order_date
                        """)
    )
    
df_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("project_1.gold.gold_customer_activity")