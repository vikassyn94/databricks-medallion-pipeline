-- Databricks notebook source
-- DBTITLE 1,Bronze Orchestration Tables
CREATE TABLE IF NOT EXISTS project_1.orchestration.bronze_batches (
  batch_id STRING,
  partition_date STRING,
  status STRING, -- SUCCESS / FAILED / PARTIAL_SUCCESS
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  processing_time_in_seconds FLOAT,
  is_retry STRING,
  no_of_attempts INT,
  run_id STRING,
  run_date DATE
)
USING DELTA;


CREATE TABLE IF NOT EXISTS project_1.orchestration.bronze_table_batches (
  batch_id STRING,
  table_name STRING,
  partition_date STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  error_message STRING,
  run_id STRING
)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,Silver Orchestration Tables
CREATE TABLE IF NOT EXISTS project_1.orchestration.silver_batches (
  batch_id STRING,
  partition_date STRING,
  status STRING, -- SUCCESS / FAILED / PARTIAL_SUCCESS
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  processing_time_in_seconds FLOAT,
  run_id STRING,
  run_date DATE
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.orchestration.silver_table_batches (
  batch_id STRING,
  table_name STRING,
  partition_date STRING,
  status STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  error_message STRING,
  run_id STRING
)
USING DELTA;