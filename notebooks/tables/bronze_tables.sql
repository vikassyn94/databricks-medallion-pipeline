-- Databricks notebook source
-- DBTITLE 1,Customers
CREATE TABLE IF NOT EXISTS project_1.bronze.customers (
  customer_id STRING,
  customer_name STRING,
  city STRING,
  phone STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.bronze.customers_quarantine (
    customer_id STRING,
    customer_name STRING,
    city STRING,
    phone STRING,
    updated_at STRING,
    ingestion_ts TIMESTAMP,
    batch_id STRING,
    quarantine_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Restaurants
CREATE TABLE IF NOT EXISTS project_1.bronze.restaurants (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.bronze.restaurants_quarantine (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _batch_id STRING,
  quarantine_reason STRING,
  quarantined_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Order_Items
CREATE TABLE IF NOT EXISTS project_1.bronze.order_items (
  order_item_id STRING,
  order_id STRING,
  restaurant_id STRING,
  item_name STRING,
  quantity STRING,
  price STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.bronze.order_items_quarantine (
  order_item_id STRING,
  order_id STRING,
  restaurant_id STRING,
  item_name STRING,
  quantity STRING,
  price STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  quarantine_reason STRING,
  quarantined_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Orders
CREATE TABLE IF NOT EXISTS project_1.bronze.orders (
  order_id STRING,
  customer_id STRING,
  order_date STRING,
  amount STRING,
  payment_status STRING,
  order_status STRING,
  updated_at STRING,
  restaurant_id STRING,
  partner_id STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.bronze.orders_quarantine (
  order_id STRING,
  customer_id STRING,
  order_date STRING,
  amount STRING,
  payment_status STRING,
  order_status STRING,
  updated_at STRING,
  restaurant_id STRING,
  partner_id STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  quarantine_reason STRING,
  quarantined_at TIMESTAMP
)
USING DELTA;


-- COMMAND ----------

-- DBTITLE 1,Deliver_Partners
CREATE TABLE project_1.bronze.delivery_partners (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE
)
USING DELTA;

CREATE TABLE project_1.bronze.delivery_partners_quarantine (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  quarantine_reason STRING,
  quarantined_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ALTER TABLE project_1.monitoring.bronze_audit_files SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');