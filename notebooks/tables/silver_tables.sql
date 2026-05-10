-- Databricks notebook source
-- DBTITLE 1,Customers
CREATE TABLE IF NOT EXISTS project_1.silver.customers (
    customer_id STRING NOT NULL,
    customer_name STRING,
    city STRING,
    phone STRING,
    updated_at TIMESTAMP,
    
    -- Lineage Metadata
    _bronze_batch_id STRING,
    _bronze_ingestion_ts TIMESTAMP,
    _silver_updated_ts TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.silver.customers_quarantine (
    customer_id STRING,
    customer_name STRING,
    city STRING,
    phone STRING,
    bronze_updated_at TIMESTAMP,

    quarantine_reason STRING,

    _bronze_batch_id STRING,
    _bronze_ingestion_ts TIMESTAMP,
    _silver_quarantined_ts TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Restaurants
CREATE TABLE IF NOT EXISTS project_1.silver.restaurants (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS project_1.silver.restaurants_quarantine (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  bronze_updated_at TIMESTAMP,
  quarantine_reason STRING,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_quarantined_ts TIMESTAMP
)
USING DELTA;


-- COMMAND ----------

-- DBTITLE 1,Order_Items
CREATE TABLE IF NOT EXISTS project_1.silver.order_items (
  order_item_id STRING,
  order_id STRING,
  restaurant_id STRING,
  item_name STRING,
  quantity INT,
  price DOUBLE,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.silver.order_items_quarantine (
  order_item_id STRING,
  order_id STRING,
  restaurant_id STRING,
  item_name STRING,
  quantity INT,
  price DOUBLE,
  bronze_updated_at TIMESTAMP,
  quarantine_reason STRING,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_quarantined_ts TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Orders
CREATE TABLE IF NOT EXISTS project_1.silver.orders (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  amount DOUBLE,
  payment_status STRING,
  order_status STRING,
  updated_at TIMESTAMP,
  restaurant_id STRING,
  partner_id STRING,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.silver.orders_quarantine (
  order_id STRING,
  customer_id STRING,
  order_date DATE,
  amount DOUBLE,
  payment_status STRING,
  order_status STRING,
  bronze_updated_at TIMESTAMP,
  restaurant_id STRING,
  partner_id STRING,
  quarantine_reason STRING,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_quarantined_ts TIMESTAMP
)
USING DELTA;


-- COMMAND ----------

-- DBTITLE 1,Delivery_Partners
CREATE TABLE project_1.silver.delivery_partners (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating DOUBLE,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA;

CREATE TABLE project_1.silver.delivery_partners_quarantine (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating DOUBLE,
  bronze_updated_at TIMESTAMP,
  quarantine_reason STRING,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_quarantined_ts TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS project_1.admin;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS project_1.admin.platform_config (
    pipeline_name STRING,
    config_key STRING,
    config_value STRING,
    updated_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

MERGE INTO project_1.admin.platform_config AS target
USING (
  SELECT
    'restaurants_silver' AS pipeline_name,
    'invalid_threshold_pct' AS config_key,
    '30' AS config_value,
    current_timestamp() AS updated_at
) AS source
ON target.pipeline_name = source.pipeline_name
AND target.config_key = source.config_key

WHEN MATCHED THEN
  UPDATE SET
    config_value = source.config_value,
    updated_at = source.updated_at

WHEN NOT MATCHED THEN
  INSERT (
    pipeline_name,
    config_key,
    config_value,
    updated_at
  )
  VALUES (
    source.pipeline_name,
    source.config_key,
    source.config_value,
    source.updated_at
  );

-- COMMAND ----------

SELECT * FROM project_1.admin.platform_config;

-- COMMAND ----------

