# Databricks notebook source
# DBTITLE 1,Import modules
spark
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Fetching Batch_id & Creating Logs Function
## Fetch the batch_id from batch_orchestrator notebook
from datetime import datetime

dbutils.widgets.text("batch_id","")
dbutils.widgets.text("table_name","")
dbutils.widgets.text("partition_date","")

batch_id = dbutils.widgets.get("batch_id")
table_name = dbutils.widgets.get("table_name")
partition_date = dbutils.widgets.get("partition_date")

## Logging utility
def log_event(step_name, message, level="INFO"):
    spark.createDataFrame(
        [Row(
            batch_id=batch_id,
            table_name=table_name,
            step_name=step_name, 
            message=message, 
            level=level,
            )],
    ).withColumn("log_ts", current_timestamp())\
    .write.mode("append")\
    .insertInto("project_1.monitoring.bronze_logs")

# COMMAND ----------

# DBTITLE 1,Phase 1: Audit Start
log_event("PHASE_1_AUDIT_START", "Marking partitions as STARTED")

file_path = f"dbfs:/Volumes/project_1/raw_dump/{table_name}/date={partition_date}"

audit_started_df = (
  spark.createDataFrame(
      [(table_name, partition_date, file_path)],
      ["table_name", "partition_date", "file_path"]
  )
  .withColumn("ingested_at", current_timestamp())
  .withColumn("batch_id", lit(batch_id))
  .withColumn("status", lit("STARTED"))
)

audit_started_df.write.mode("append").insertInto(
  "project_1.monitoring.bronze_audit_files"
  )

# COMMAND ----------

# DBTITLE 1,Phase 2: Read
try:
    customers_df = (
        spark.read
            .format("parquet")
            .load(file_path)
            .withColumn("phone", col("phone").cast("string"))
            .withColumn("updated_at", col("updated_at").cast("string"))
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_batch_id", lit(batch_id))
            .withColumn("_ingest_date", current_date())
    )

    log_event("PHASE_2_READ", f"Successfully read {table_name} files")

except Exception as e:
    log_event("PHASE_2_READ", str(e), "ERROR")
    raise

# COMMAND ----------

# DBTITLE 1,Retry Logic: Idempotency
## Idempotency check

# Check of already ingested file into main table
existing_files = (
    spark.table(f"project_1.bronze.{table_name}")
    .select("_source_file")
    .distinct()
)

new_df = customers_df.join(
    existing_files,
    on="_source_file",
    how="leftanti"
)

is_skipped = False

if new_df.limit(1).count() == 0:
    is_skipped = True
    
    metrics_exist = (
            spark.table("project_1.monitoring.bronze_audit_files")
            .filter(
                (col("partition_date") == partition_date) &
                (col("table_name") == table_name)
            )
            .limit(1)
            .count()
        )

if is_skipped and metrics_exist == 0:

    log_event("PHASE_METRICS_RECOVERY", "Recomputing metrics from Bronze table", "RETRY")

    recovered_df = (
        spark.table(f"project_1.bronze.{table_name}")
        .filter(col("_source_file").contains(partition_date))
    )
    
    prev_batch_id = recovered_df.select(col("_batch_id")).distinct().collect()[0][0]

    file_metrics_df = (
        recovered_df
        .groupBy("_source_file")
        .agg(
            count("*").alias("source_rows"),
            sum(when(col("quarantine_reason").isNull(),1).otherwise(0)).alias("ingested_rows"),
            sum(when(col("quarantine_reason").isNotNull(),1).otherwise(0)).alias("quarantined_rows")
            )
        .withColumn("batch_id", prev_batch_id)
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("table_name", lit(table_name))
    )

    # Log Metrics
    source_count = file_metrics_df.agg(sum(col("source_rows")).alias("source_rows")).collect()[0]["source_rows"]
    
    good_count = file_metrics_df.agg(sum(col("ingested_rows")).alias("ingested_rows")).collect()[0]["ingested_rows"]

    bad_count = file_metrics_df.agg(sum(col("quarantined_rows")).alias("quarantined_rows")).collect()[0]["quarantined_rows"]
    


    #Update bronze_audit_files table
    spark.sql(f"""
    UPDATE project_1.monitoring.bronze_audit_files
    SET status = 'SUCCESS',
        ingested_at = current_timestamp()
    WHERE table_name = '{table_name}'
    AND partition_date = '{partition_date}'
    AND status = 'STARTED'
    AND batch_id = '{prev_batch_id}'
    """)

    log_event("PHASE_AUDIT_UPDATE", "Audit update successful", "RETRY")


    #Add to bronze_metrics table
    bronze_metrics_df = (
        spark.createDataFrame(
            [(prev_batch_id, source_count, good_count, bad_count)],
            ["batch_id", "source_rows", "ingested_rows", "quarantined_rows"]
        ).withColumn("ingestion_ts", current_timestamp())
        .withColumn("_ingest_date", current_date())
        .withColumn("table_name", lit(table_name))
        .select(
            col("ingestion_date"),
            col("table_name"),
            col("batch_id"),
            col("ingestion_ts"),
            col("source_rows"),
            col("ingested_rows"),
            col("quarantined_rows")
            )
    )

    bronze_metrics_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("project_1.monitoring.bronze_metrics")
    
    log_event("PHASE_METRICS_UPDATE", "Metrics update successful", "RETRY")


    #Add to bronze_file_metrics table
    file_metrics_df_out = file_metrics_df.select(
        col("_source_file").alias("file_path"),
        col("table_name"),
        col("batch_id"),
        col("ingestion_ts"),
        col("source_rows"),
        col("ingested_rows"),
        col("quarantined_rows")
    )

    file_metrics_df_out.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("project_1.monitoring.bronze_file_metrics")

    log_event("PHASE_FILE_METRICS_UPDATE", "File Metrics update successful", "RETRY")

    log_event("PHASE_PIPELINE END", "Bronze ingestion completed successfully", "RETRY")
    print("Bronze ingestion completed successfully")

    dbutils.notebook.exit("ALREADY_PROCESSED")
    

# COMMAND ----------

# DBTITLE 1,Phase 3: Schema Validation
## Historical Schema Drift Detection

incoming_schema = customers_df.schema.json()

schema_df = spark.createDataFrame(
    [(table_name, incoming_schema, batch_id)],
    ["table_name", "schema_json", "batch_id"]
).withColumn("detected_at", current_timestamp())

schema_df.write.mode("append").saveAsTable("project_1.monitoring.bronze_schema_registry")

log_event("PHASE_3_SCHEMA_DRIFT_VALIDATION", "Incoming schema captured")

previous_schema_row = (
    spark.table("project_1.monitoring.bronze_schema_registry")\
    .filter (col("table_name") == table_name)\
    .orderBy(desc("detected_at"))\
    .limit(2)\
    .collect()
)

display(previous_schema_row)
print(len(previous_schema_row))

if len(previous_schema_row) == 2:
    previous_schema = previous_schema_row[1]["schema_json"]
    current_schema = previous_schema_row[0]["schema_json"]

    if previous_schema != current_schema:
        log_event("PHASE_3_SCHEMA_DRIFT_VALIDATION", "Schema drift detected between batches", "WARN")
else:
    log_event("PHASE_3_SCHEMA_DRIFT_VALIDATION", "Baseline Schema captured")


def extract_columns(schema_json):
    import json
    schema = json.loads(schema_json)
    return {field["name"] for field in schema["fields"]}

if len(previous_schema_row) == 2:
    prev_cols = extract_columns(previous_schema)
    curr_cols = extract_columns(current_schema)
    added_cols = curr_cols - prev_cols
    removed_cols = prev_cols - curr_cols

    if removed_cols:
        log_event("PHASE_3_SCHEMA_DRIFT_VALIDATION", f"Removed columns: {removed_cols}", "WARN")
    if added_cols:
        log_event("PHASE_3_SCHEMA_DRIFT_VALIDATION", f"Added columns: {added_cols}", "WARN")


# COMMAND ----------

# DBTITLE 1,Phase 4: Validation
## Contract Enforcement

# Row count check (CRITICAL)

row_count = customers_df.count()

if row_count == 0:
    log_event("PHASE_4_VALIDATION", "Zero rows detected", "ERROR")
    raise Exception("Validation failed: Empty dataset")
log_event("PHASE_4_VALIDATION", f"Row count validation passed: {row_count}")

# Mandatory columns check

mandatory_cols = ("customer_id", "customer_name", "city", "phone", "updated_at")

missing = [c for c in mandatory_cols if c not in customers_df.columns]

if missing:
    log_event("PHASE_4_VALIDATION", f"Missing columns: {missing}", "ERROR")
    raise Exception("Schema Validation failed")

log_event("PHASE_4_VALIDATION", "Schema validation passed")

# COMMAND ----------

# DBTITLE 1,Phase 5: Quarantine
## Define Data Quality Rules

#customer_id IS NOT NULL
#updated_at can be parsed to timestamp
#phone length = 10
#no blank strings

log_event("PHASE_5_QUARANTINE", "Performing Data Quality Checks")

# Quarantine Rules
validated_df = (
    customers_df
    .withColumn(
        "quarantine_reason",
        when(col("customer_id").isNull(), "NULL_CUSTOMER_ID")
        .when(trim(col("customer_id")) == "", "EMPTY_CUSTOMER_ID")
        .when(col("updated_at").isNull(), "NULL_UPDATED_AT")
        .when(trim(col("updated_at")) == "", "EMPTY_UPDATED_AT")
        .when(length(col("phone")) != 10, "INVALID_PHONE")
            )
        )

# Split Good vs Bad Rows
good_rows = validated_df.filter(col("quarantine_reason").isNull())
bad_rows = validated_df.filter(col("quarantine_reason").isNotNull())

# Calculate Metrics
file_metrics_df = (
    validated_df
    .groupBy("_source_file")
    .agg(
        count("*").alias("source_rows"),
        sum(when(col("quarantine_reason").isNull(),1).otherwise(0)).alias("ingested_rows"),
        sum(when(col("quarantine_reason").isNotNull(),1).otherwise(0)).alias("quarantined_rows")
        )
    .withColumn("batch_id", lit(batch_id))
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("table_name", lit(table_name))
)

# Log Metrics
source_count = file_metrics_df.agg(sum(col("source_rows")).alias("source_rows")).collect()[0]["source_rows"]
good_count = file_metrics_df.agg(sum(col("ingested_rows")).alias("ingested_rows")).collect()[0]["ingested_rows"]
bad_count = file_metrics_df.agg(sum(col("quarantined_rows")).alias("quarantined_rows")).collect()[0]["quarantined_rows"]



# Quarantine Bad Rows to Quarantine Table
if bad_count > 0:
    quarantine_df = (
        bad_rows
        .withColumn("quarantined_at", current_timestamp())
        .drop("_source_file", "_ingest_date")
        )
    quarantine_df.write\
        .format("delta")\
        .option("mergeSchema", "true")\
        .mode("append")\
        .saveAsTable(f"project_1.bronze.{table_name}_quarantine")

log_event("PHASE_5_QUARANTINE", f"{bad_count} rows quarantined")


# COMMAND ----------

# DBTITLE 1,Phase 6: Write
# Only Write Good Rows to Bronze
log_event("PHASE_6_WRITE", f"Writing data to {table_name}")

try:
    # Drop 'quarantine_reason' column before writing to bronze table
    good_rows_bronze = good_rows.drop("quarantine_reason")
    good_rows_bronze.write\
        .format("delta")\
        .option("mergeSchema", "true")\
        .mode("append")\
        .saveAsTable(f"project_1.bronze.{table_name}")

    log_event("PHASE_6_WRITE", f"Bronze {table_name} write successful")
    
except Exception as e:
    log_event("PHASE_6_WRITE", str(e), "ERROR")
    raise

# COMMAND ----------

# DBTITLE 1,Phase 7: Post Validate
written_rows = (
    spark.table(f"project_1.bronze.{table_name}")
    .filter (col("_batch_id") == batch_id)
    .count()
)

log_event("PHASE_7_POST_VALIDATE", f"Rows written for batch {batch_id}: {written_rows}")

if written_rows == 0:
    raise Exception ("Write succeeded but 0 rows found in bronze table")

# COMMAND ----------

# DBTITLE 1,Phase 8: Audit
log_event("PHASE_8_AUDIT", "Recording ingested files")

spark.sql(f"""
UPDATE project_1.monitoring.bronze_audit_files
SET status = 'SUCCESS',
    ingested_at = current_timestamp()
WHERE table_name = '{table_name}'
  AND partition_date = '{partition_date}'
  AND batch_id = '{batch_id}'
  AND status = 'STARTED'
""")

log_event("PHASE_8_AUDIT", "Audit commit completed")

# COMMAND ----------

# DBTITLE 1,Phase 9: Ingestion Metrics
## File-Level & Batch-Level Metrics. 

# Select only columns matching the table schema
file_metrics_df_out = file_metrics_df.select(
    col("_source_file").alias("file_path"),
    col("table_name"),
    col("batch_id"),
    col("ingestion_ts"),
    col("source_rows"),
    col("ingested_rows"),
    col("quarantined_rows")
)

metrics_df_out = (
    spark.createDataFrame(
        [(batch_id, source_count, good_count, bad_count)],
        ["batch_id", "source_rows", "ingested_rows", "quarantined_rows"]
    )
    .withColumn("ingestion_date", current_date())
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("table_name", lit(table_name))
    .select(
        col("ingestion_date"),
        col("table_name"),
        col("batch_id"),
        col("ingestion_ts"),
        col("source_rows"),
        col("ingested_rows"),
        col("quarantined_rows")
    )
)

#Write to metrics tables
file_metrics_df_out.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("project_1.monitoring.bronze_file_metrics")

metrics_df_out.write\
    .format("delta")\
    .mode("append")\
    .saveAsTable("project_1.monitoring.bronze_metrics")


log_event("PHASE_9_INGESTION_METRICS", "Batch & File Ingestion metrics recorded")

#validated_df.unpersist() # Clean Up Memory

# COMMAND ----------

# DBTITLE 1,Phase 10: Pipeline End
log_event("PHASE_10_PIPELINE END", "Bronze ingestion completed successfully")
print("Bronze ingestion completed successfully")