-- Databricks notebook source
-- DBTITLE 1,Silver Customers Temp Table
CREATE TABLE IF NOT EXISTS project_1.temp.customers_silver_input_df (
  customer_id STRING,
  customer_name STRING,
  city STRING,
  phone STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE,
  parsed_date TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.customers_silver_valid_df (
  customer_id STRING,
  customer_name STRING,
  city STRING,
  phone STRING,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.customers_silver_dedup_df (
  customer_id STRING,
  customer_name STRING,
  city STRING,
  phone STRING,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);

-- COMMAND ----------

-- DBTITLE 1,Silver Orders Temp Table
CREATE TABLE IF NOT EXISTS project_1.temp.orders_silver_input_df (
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
  _ingest_date DATE,
  parsed_date TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.orders_silver_valid_df (
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
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.orders_silver_dedup_df (
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
USING DELTA
PARTITIONED BY (_bronze_batch_id);

-- COMMAND ----------

-- DBTITLE 1,Silver Restaurants Temp Table
CREATE TABLE IF NOT EXISTS project_1.temp.restaurants_silver_input_df (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE,
  parsed_date TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_batch_id);


CREATE TABLE IF NOT EXISTS project_1.temp.restaurants_silver_valid_df (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.restaurants_silver_dedup_df (
  restaurant_id STRING,
  restaurant_name STRING,
  city STRING,
  cuisine STRING,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);

-- COMMAND ----------

-- DBTITLE 1,Silver Order_Items Temp Table
  
CREATE TABLE IF NOT EXISTS project_1.temp.order_items_silver_input_df (
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
  _ingest_date DATE,
  parsed_date TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.order_items_silver_valid_df (
  order_item_id STRING,
  order_id STRING,
  restaurant_id STRING,
  item_name STRING,
  quantity INTEGER,
  price DOUBLE,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);

CREATE TABLE IF NOT EXISTS project_1.temp.order_items_silver_dedup_df (
  order_item_id STRING,
  order_id STRING,
  restaurant_id STRING,
  item_name STRING,
  quantity INTEGER,
  price DOUBLE,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (_bronze_batch_id)

-- COMMAND ----------

-- DBTITLE 1,Silver Delivery_Partners Temp Table
CREATE TABLE IF NOT EXISTS project_1.temp.delivery_partners_silver_input_df (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating DOUBLE,
  updated_at STRING,
  _ingestion_ts TIMESTAMP,
  _source_file STRING,
  _batch_id STRING,
  _ingest_date DATE,
  parsed_date TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_batch_id);


CREATE TABLE IF NOT EXISTS project_1.temp.delivery_partners_silver_valid_df (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating DOUBLE,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP,
  quarantine_reason STRING
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);


CREATE TABLE IF NOT EXISTS project_1.temp.delivery_partners_silver_dedup_df (
  partner_id STRING,
  partner_name STRING,
  city STRING,
  rating DOUBLE,
  updated_at TIMESTAMP,
  _bronze_batch_id STRING,
  _bronze_ingestion_ts TIMESTAMP,
  _silver_updated_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (_bronze_batch_id);
