# Databricks notebook source
# MAGIC %run
# MAGIC /<your_slack_alert_notebook_path>

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime
import uuid

run_id = f"RUN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
run_date = datetime.now().date()

print(f"Run ID: {run_id}")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, TimestampType, FloatType, IntegerType, DateType

b_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("partition_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("processing_time_in_seconds", FloatType(), True),
    StructField("is_retry", StringType(), True),
    StructField("no_of_attempts", IntegerType(), True),
    StructField("run_id", StringType(), True),
    StructField("run_date", DateType(), True)
])

tb_schema = StructType([
    StructField("batch_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("partition_date", StringType(), True),   
    StructField("status", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("error_message", StringType(), True),
    StructField("run_id", StringType(), True)
])

def table_batches_in(batch_id, table_name, partition_date, status, start_time, end_time, error_message=None, run_id=run_id):
    
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
    ).write.mode("append").insertInto("project_1.orchestration.bronze_table_batches")

# COMMAND ----------

tables = ["customers", "orders", "order_items", "restaurants", "delivery_partners"]

all_partitions = []

for table in tables:
    
    base_path = f"/Volumes/project_1/raw_dump/{table}/"
    
    try:
        paths = dbutils.fs.ls(base_path)
        
        for p in paths:
            if "date=" in p.path:
                partition_date = p.path.split("date=")[-1].strip("/")
                all_partitions.append((table, partition_date))
    
    except:
        print(f"Could not read {table}")

partitions_df = spark.createDataFrame(all_partitions, ["table_name","partition_date"]).distinct()

processed = (
    spark.table("project_1.orchestration.bronze_table_batches")
    .filter(col("status") == "SUCCESS")
    .select("table_name", "partition_date")
    .distinct()
)


pending_work_df = (
    partitions_df.join(processed, on=["table_name", "partition_date"], how="leftanti")
)


grouped_df = (
    pending_work_df
    .groupBy(col("partition_date")).agg(collect_list(col("table_name")).alias("tables"))
)

partition_dt_lst = grouped_df.collect()

if len(partition_dt_lst) == 0:
    print("No work to do")
    dbutils.notebook.exit("No work to do")
else:
    print("Work to do")
    print(f"Partitions to process: {len(partition_dt_lst)}")


# COMMAND ----------

run_summary = []

for row in partition_dt_lst:
    run_summary.append({
        "partition_date": row["partition_date"],
        "tables": row["tables"]
    })


# Construct Alert Message
if len(run_summary) > 0:

    message = f"""
📥 FILE ARRIVAL ALERT (BRONZE)

Total Partitions: {len(run_summary)}

"""

    for item in run_summary:
        message += f"""
        📅 Partition: {item['partition_date']}
        📦 Tables: {", ".join(item['tables'])}
        """

        message += "\n🚀 Bronze ingestion starting..."

    send_alert(message)

else:
    message = f"""
    🚨 FILE ARRIVAL ANOMALY

    No partitions detected but Bronze job triggered.

    ⚠️ Possible issues:
    - Empty folder write
    - Incorrect trigger path
    """

    send_alert(message)

# COMMAND ----------

for row in partition_dt_lst:
    results = {}

    try:
        partition_date = row["partition_date"]
        tables_to_run = row["tables"]
        
        # Check existing batch for partition
        existing_batch = (
            spark.table("project_1.orchestration.bronze_batches")
            .filter(col("partition_date") == partition_date)
            .orderBy(col("start_time").desc())
            .limit(1)
            .collect()  
        )

        # Check existing batch for partition
        
        if len(existing_batch) > 0:
            last_batch = existing_batch[0]

            if last_batch["status"] in ["FAILED", "PARTIAL_SUCCESS"]:
                # RETRY CASE
                batch_id = last_batch["batch_id"]
                is_retry = "YES"
                attempts = (last_batch["no_of_attempts"] or 1) + 1

                print(f"Retrying batch: {batch_id}")
                print(f"Tables: {tables_to_run}")

                batch_start_time = datetime.now()

                # Update retry metadata
                spark.sql(f"""
                UPDATE project_1.orchestration.bronze_batches
                SET
                    is_retry = 'YES',
                    no_of_attempts = {attempts},
                    start_time = current_timestamp(),
                    status = 'STARTED'
                WHERE batch_id = '{batch_id}'
                """)

    
            else:
                # Already SUCCESS → skip
                print(f"Partition {partition_date} already completed")
                continue

        else:
            # NEW BATCH
            batch_id = f"BATCH_{partition_date}_{uuid.uuid4().hex[:6]}"
            is_retry = "NO"
            attempts = 1

            batch_start_time = datetime.now()
            
            print(f"\nProcessing partition: {partition_date}")
            print(f"Tables: {tables_to_run}")

            spark.createDataFrame(
            [(batch_id, partition_date, "STARTED", batch_start_time, None, None, is_retry, attempts, run_id, run_date)],
            schema=b_schema
            ).write.mode("append").insertInto("project_1.orchestration.bronze_batches")


        for table in tables_to_run:

            try:
                print(f"Running {table}")

                start_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

                dbutils.notebook.run(
                    f"/Workspace/Users/vikas.singh.da@gmail.com/ETL_Project/Bronze/bronze_layer_{table}",
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

                table_batches_in(batch_id, table, partition_date, "SUCCESS", start_time, end_time)
                

            except Exception as e:
                print(f"Failed {table}: {str(e)}")
                results[table] = "FAILED"

                end_time = spark.sql("SELECT current_timestamp()").collect()[0][0]

                table_batches_in(batch_id, table, partition_date, "FAILED", start_time, end_time, str(e))
                
    except Exception as e:
        print(f"Error: for partition {partition_date}: {str(e)}")
        
    finally:
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
            UPDATE project_1.orchestration.bronze_batches
            SET
                status = '{final_status}',
                end_time = '{batch_end_time}',
                processing_time_in_seconds = {processing_time_in_seconds}
            WHERE batch_id = '{batch_id}'
            """)
            print(f"Batch {batch_id} completed with status {final_status}")
           
print(f"Pipeline run completed")

# COMMAND ----------


# ================================
# FETCH BRONZE BATCH SUMMARY
# ================================

bronze_batches_df = (
    spark.table("project_1.orchestration.bronze_batches")
    .filter(col("run_id") == run_id)
)

total_batches = bronze_batches_df.count()

success_batches = bronze_batches_df.filter(col("status") == "SUCCESS").count()
partial_batches = bronze_batches_df.filter(col("status") == "PARTIAL_SUCCESS").count()
failed_batches = bronze_batches_df.filter(col("status") == "FAILED").count()

# ================================
# TABLE LEVEL FAILURES
# ================================

failed_tables_df = (
    spark.table("project_1.orchestration.bronze_table_batches")
    .filter(col("run_id") == run_id)
    .filter(col("status") != "SUCCESS")
)

failed_tables = failed_tables_df.select(
    "table_name", "partition_date", "error_message"
).collect()

# ================================
# FORMAT FAILED TABLE DETAILS
# ================================

failed_table_msg = ""

if failed_tables:
    failed_table_msg += "❌ FAILED TABLES:\n"
    for row in failed_tables:
        failed_table_msg += f"- {row['table_name']} | {row['partition_date']} → {row['error_message']}\n"
else:
    failed_table_msg += "✅ No table-level failures\n"

# ================================
# FAILED / PARTIAL PARTITIONS
# ================================

failed_partitions = (
    bronze_batches_df
    .filter(col("status") != "SUCCESS")
    .select("partition_date")
    .distinct()
    .collect()
)

partition_msg = ""

if failed_partitions:
    partition_msg += "\n📅 AFFECTED PARTITIONS:\n"
    for row in failed_partitions:
        partition_msg += f"- {row['partition_date']}\n"

# ================================
# FINAL STATUS
# ================================

if failed_batches > 0:
    overall_status = "❌ FAILED"
elif partial_batches > 0:
    overall_status = "⚠️ PARTIAL SUCCESS"
else:
    overall_status = "✅ SUCCESS"

# ================================
# FINAL MESSAGE
# ================================

message = f"""
🧱 BRONZE ORCHESTRATION ALERT

Run ID: {run_id}
Status: {overall_status}

================ SUMMARY =================
Total Batches: {total_batches}
✅ Success: {success_batches}
⚠️ Partial: {partial_batches}
❌ Failed: {failed_batches}

================ DETAILS =================
{failed_table_msg}
{partition_msg}
"""

# ================================
# SEND ALERT
# ================================

if overall_status == "❌ FAILED":
    send_alert(message, "ERROR")
elif overall_status == "⚠️ PARTIAL SUCCESS":
    send_alert(message, "WARNING")
else:
    send_alert(message, "INFO")

# if final_status == "FAILED":
#     print(f"Bronze failed with status: {final_status}")
#     dbutils.notebook.exit("FAILED")
# else:
#     dbutils.notebook.exit("SUCCESS")