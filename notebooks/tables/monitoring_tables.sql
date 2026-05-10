-- Databricks notebook source
-- DBTITLE 1,Bronze Monitoring Tables
-- BRONZE LAYER MONITORING TABLES --

CREATE TABLE IF NOT EXISTS project_1.monitoring.bronze_audit_files (
  table_name STRING,
  partition_date STRING,
  file_path STRING,
  ingested_at TIMESTAMP,
  batch_id STRING,
  status STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS project_1.monitoring.bronze_logs (
    batch_id      STRING,
    table_name    STRING,
    step_name     STRING,
    message       STRING,
    log_level     STRING,
    log_ts        TIMESTAMP
)
USING DELTA;


CREATE TABLE IF NOT EXISTS project_1.monitoring.bronze_schema_registry (
    table_name STRING,
    schema_json STRING,
    detected_at TIMESTAMP,
    batch_id STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS project_1.monitoring.bronze_metrics (
    ingestion_date DATE,
    table_name STRING,
    batch_id STRING,
    ingestion_ts TIMESTAMP,
    source_rows LONG,
    ingested_rows LONG,
    quarantined_rows LONG
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.monitoring.bronze_file_metrics (
    file_path STRING,
    table_name STRING,
    batch_id STRING,
    ingestion_ts TIMESTAMP,
    source_rows LONG,
    ingested_rows LONG,
    quarantined_rows LONG
)
USING DELTA;



-- COMMAND ----------

-- DBTITLE 1,Silver Monitoring Tables
-- SILVER LAYER MONITORING TABLES --

CREATE TABLE IF NOT EXISTS project_1.monitoring.silver_batch_metrics (
    run_id STRING,
    batch_id STRING,
    source_name STRING,
    
    bronze_rows_read BIGINT,
    after_business_validation BIGINT,
    deduplicated_rows BIGINT,
    inserted_rows BIGINT,
    updated_rows BIGINT,
    stale_rows BIGINT,
    late_rows BIGINT,
    quarantined_rows BIGINT,

    processed_at TIMESTAMP,
    status STRING,
    processing_time_seconds FLOAT,
    last_completed_phase STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS project_1.monitoring.silver_logs (
    batch_id STRING,
    table_name STRING,
    step_name STRING,
    message STRING,
    log_level STRING,
    log_ts TIMESTAMP
)
USING DELTA;



-- COMMAND ----------

-- DBTITLE 1,Silver Batch View
CREATE OR REPLACE VIEW project_1.monitoring.silver_batch_view AS
SELECT
batch_id,
source_name,
status,
processed_at,
bronze_rows_read,
after_business_validation,
quarantined_rows,
inserted_rows,
updated_rows,
stale_rows,
late_rows,
processing_time_seconds,
(quarantined_rows / bronze_rows_read) * 100 AS invalid_pct,
(updated_rows * 100.0 / deduplicated_rows) AS update_ratio,
(bronze_rows_read / processing_time_seconds) AS rows_per_second
FROM project_1.monitoring.silver_batch_metrics
ORDER BY processed_at;


-- COMMAND ----------

-- DBTITLE 1,Silver Silent Failure Table
CREATE TABLE IF NOT EXISTS project_1.monitoring.silver_silent_failure_records (
    
    run_id STRING,
    run_date DATE,
    batch_id STRING,
    table_name STRING,
    detected_at TIMESTAMP,
    status_after_debug STRING,
    debug_notes STRING

)
USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,API Ingestion Table
CREATE TABLE project_1.monitoring.api_ingestion_tracker (
    source_name STRING,
    file_date DATE,
    ingestion_status STRING,
    rows_ingested BIGINT,
    raw_path STRING,
    ingestion_ts TIMESTAMP
)
USING DELTA

-- COMMAND ----------

