-- Databricks notebook source
-- Silver Layer Health (Pie chart)

SELECT status, COUNT(*) as batches
FROM silver_batch_view
GROUP BY status;

-- COMMAND ----------

--Silver Data Quality Trend (Line Chart)

SELECT processed_at, NULLIF(AVG(invalid_pct),0) AS invalid_pct
FROM silver_batch_view
GROUP BY processed_at;

-- COMMAND ----------

--Silver Data Change Behaviour (Stacked Bar Chart)

SELECT processed_at, SUM(inserted_rows) AS inserted_rows, SUM(updated_rows) AS updated_rows, SUM(stale_rows) AS stale_rows, SUM(late_rows) AS late_rows
FROM silver_batch_view
GROUP BY processed_at;

-- COMMAND ----------

--Silver Layer Performance (Line Chart)

SELECT processed_at::date AS processed_at, AVG(rows_per_second) AS rows_per_second
FROM silver_batch_view
GROUP BY processed_at::date;

-- COMMAND ----------

--Bronze Data Change Behaviour (Stacked Bar Chart)

SELECT *
FROM project_1.monitoring.bronze_metrics;

-- COMMAND ----------

--Bronze Layer Health (Pie chart)

SELECT status, COUNT(*) AS total_count
FROM project_1.monitoring.bronze_audit_files
GROUP BY status;