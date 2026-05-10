# Databricks notebook source
# MAGIC %run
# MAGIC <your_slack_alert_notebook_path>

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, DateType
from datetime import datetime


# Batches table
b_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("partition_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("processing_time_in_seconds", FloatType(), True),
    StructField("run_id", StringType(), True),
    StructField("run_date", DateType(), True)
])

# Table_batches table
tb_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("table_name", StringType(),True),
    StructField("partition_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("error_message", StringType(), True),
    StructField("run_id", StringType(), True)
])

def table_batches_in(batch_id, table_name, partition_date, status, start_time, end_time, error_message=None, run_id=None):
    spark.createDataFrame(
        [(
            batch_id,
            table_name,
            partition_date,
            status,
            start_time,
            end_time,
            error_message,
            run_id
        )],
        schema=tb_schema
        ).write.mode("append").insertInto("project_1.orchestration.silver_table_batches")

# COMMAND ----------

# DBTITLE 1,Cell 2
# Step 1: Bronze successful table-level data

bronze_success = (
    spark.table("project_1.orchestration.bronze_batches")
    .filter(col("status") == "SUCCESS")
    .select("batch_id", "partition_date", "run_id")
)

# Step 2: Silver processed table-level data

silver_processed = (
    spark.table("project_1.orchestration.silver_batches")
    .filter(col("status") == "SUCCESS")
    .select("batch_id", "partition_date", "run_id")
)

# Step 3: Find pending (THIS is the fix)

pending = (
    bronze_success.alias("b")
    .join(
        silver_processed.alias("s"),
        on="batch_id",
        how="leftanti"
    )
)

bronze_table_success = (
    spark.table("project_1.orchestration.bronze_table_batches")
    .filter(col("status") == "SUCCESS")
)

pending_batches = (
    pending.join(
        bronze_table_success,
        on="batch_id", 
        how="inner"
        ).select(col("batch_id"), col("table_name"), pending["partition_date"], pending["run_id"])
    )

grouped_df = (
    pending_batches
    .groupBy(col("batch_id"), col("partition_date"), col("run_id")).agg(collect_list(col("table_name")).alias("tables"))
)

batch_lst = grouped_df.collect()

if len(batch_lst) == 0:
    print("No pending Bronze batches to process.")
    dbutils.notebook.exit("NO_PENDING_BATCHES")
else:
    print("Work to do")
    print(f"Batches to process: {len(batch_lst)}")
print(batch_lst)

# COMMAND ----------

start_msg = f"""
🥈 SILVER ORCHESTRATION STARTED

Processing batches triggered from Bronze SUCCESS.
"""

send_alert(start_msg, "INFO")

# COMMAND ----------

for row in batch_lst:
    try:
        batch_id = row["batch_id"]
        tables_to_run = row["tables"]
        partition_date = row["partition_date"]
        run_id = row["run_id"]
        run_date = datetime.now().date()

        batch_start_time = datetime.now()

        spark.createDataFrame(
        [(batch_id, partition_date, "STARTED", batch_start_time, None, None, run_id, run_date)],
        schema=b_schema
        ).write.mode("append").insertInto("project_1.orchestration.silver_batches")

        print(f"\nProcessing batch: {batch_id}")
        print(f"Tables: {tables_to_run}")

        results = {}

        for table in tables_to_run:

            try:
                print(f"Running {table}")

                start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

                dbutils.notebook.run(
                    f"/Workspace/Users/vikas.singh.da@gmail.com/ETL_Project/Silver/silver_layer_{table}",
                    0,
                    arguments={
                        "batch_id": batch_id,
                        "table_name": table,
                        "partition_date": partition_date,
                        "run_id": run_id
                    }
                )

                results[table] = "SUCCESS"

                end_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

                table_batches_in(batch_id, table, partition_date, "SUCCESS", start_time, end_time, run_id)
                

            except Exception as e:
                print(f"Failed {table}: {str(e)}")
                results[table] = "FAILED"

                end_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

                table_batches_in(batch_id, table, partition_date, "FAILED", start_time, end_time, str(e), run_id)
                
    except Exception as e:
        print(f"Error: for partition {partition_date}: {str(e)}")
        
    if results:
        if all(v == "SUCCESS" for v in results.values()):
            final_status = "SUCCESS"
        elif any(v == "SUCCESS" for v in results.values()):
            final_status = "PARTIAL_SUCCESS"
        else:
            final_status = "FAILED"
        

        batch_end_time = datetime.now()
        processing_time_in_seconds = (batch_end_time - batch_start_time).total_seconds()

        spark.sql(f"""
        UPDATE project_1.orchestration.silver_batches
        SET
            status = '{final_status}',
            end_time = '{batch_end_time}',
            processing_time_in_seconds = {processing_time_in_seconds}
        WHERE batch_id = '{batch_id}'
        AND start_time = '{batch_start_time}'
        """)
        print(f"Batch {batch_id} completed with status {final_status}")
print(f"Pipeline run completed")

# COMMAND ----------

# DBTITLE 1,Silent failure write
failed_tables_df = (
    spark.table("project_1.monitoring.silver_batch_metrics")
    .filter(col("status") != "SUCCESS")
)

if len(failed_tables_df.collect()) > 0:

    silent_df = (
        failed_tables_df
        .withColumn("run_id", lit(run_id))
        .withColumn("run_date", lit(run_date))
        .withColumn("detected_at", current_timestamp())
        .withColumn("status_after_debug", lit("PENDING"))
        .withColumn("debug_notes", lit("NA"))
    )

    final_silent_df = (
        silent_df
        .select(
            col("run_id"),
            col("run_date"),
            col("batch_id"),
            col("source_name").alias("table_name"),
            col("detected_at"),
            col("status_after_debug"),
            col("debug_notes")
        )
    )

    existing_batches = [
        row["batch_id"] for row in spark.table("project_1.monitoring.silver_silent_failure_records")
        .filter(col("run_id") == run_id)
        .select("batch_id")
        .distinct()
        .collect()
    ]

    final_df = final_silent_df.filter(~col("batch_id").isin(existing_batches))

    final_df.write.mode("append").insertInto("project_1.monitoring.silver_silent_failure_records")
