# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

## Fething batch_id and table_name

dbutils.widgets.text("batch_id", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("partition_date", "")
dbutils.widgets.text("run_id", "")

batch_id = dbutils.widgets.get("batch_id")
table_name = dbutils.widgets.get("table_name")
partition_date = dbutils.widgets.get("partition_date")
run_id = dbutils.widgets.get("run_id")


print(f"Running Silver for batch_id: {batch_id} of run_id: {run_id}")

## Logging

def log_event(batch_id, step_name, message, level="INFO"):
  (
    spark.createDataFrame(
      [(batch_id, table_name, step_name, message, level)],
      ["batch_id", "table_name", "step_name", "message", "level"]
    )
    .withColumn("log_ts", current_timestamp())
    .write.mode("append")
    .insertInto("project_1.monitoring.silver_logs")
  )

# COMMAND ----------

## PHASE 1 — Read Config

pipeline_name = f"{table_name}_silver"

threshold = (
  spark.table("project_1.admin.platform_config")
  .filter(col("pipeline_name") == pipeline_name)
  .filter(col("config_key") == "invalid_threshold_pct")
  .orderBy(col("updated_at").desc())
  .selectExpr("cast(config_value as int) as threshold")
  .limit(1)
  .collect()[0]["threshold"]
)

print(f"Invalid threshold percentage: {threshold}%")

if threshold is None:
  raise Exception("Invalid threshold configuration missing.")

# COMMAND ----------

existing = (
    spark.table("project_1.monitoring.silver_batch_metrics")
    .filter((col("batch_id") == batch_id) & (col("source_name") == table_name) &(col("status") == "SUCCESS"))
    ).count()

if existing > 0:
    print(f"Batch {batch_id} already processed. Skipping.")
    dbutils.notebook.exit("ALREADY_PROCESSED")

# COMMAND ----------

## Phase Orchestrator
# (This pattern keeps the pipeline restartable, idempotent, and cheap to rerun without rewriting everything. Think of it like a phase orchestrator controlling which step runs.)

# Step 1: Phase Update Helper (Every phase calls this only after successful completion.)
def update_phase(batch_id, phase):
    spark.sql(f"""
              UPDATE project_1.monitoring.silver_batch_metrics
              SET
                last_completed_phase = '{phase}'
              WHERE batch_id = '{batch_id}'
              AND source_name = '{table_name}'
              """)

PHASES = {
            "PHASE_1": 1,
            "PHASE_2": 2,
            "PHASE_3": 3,
            "PHASE_4": 4,
            "PHASE_5": 5,
            "PHASE_6": 6,
            "PHASE_7": 7,
            "PHASE_8": 8,
            "PHASE_9": 9
        }

# 
def process_batch(batch_id):
    try:
        # Fetch Last Phase
        state = (
            spark.table("project_1.monitoring.silver_batch_metrics")
            .filter((col("batch_id") == batch_id) & (col("source_name") == table_name))
            .select("last_completed_phase")
            .limit(1)
            .collect()
        )

        last_phase = None

        if state and state[0]["last_completed_phase"] is not None:
            last_phase = PHASES[state[0]["last_completed_phase"]]

        print(f"Last completed phase for {batch_id}: {last_phase}")



        ## PHASE 3 — Initialize / Refresh Batch State

        # Upsert STARTED state
        spark.sql(f"""
                MERGE INTO project_1.monitoring.silver_batch_metrics AS target
                USING (
                    SELECT 
                        '{run_id}' AS run_id,
                        '{batch_id}' AS batch_id,
                        current_timestamp() AS processed_at,
                        '{table_name}' AS source_name,
                        'STARTED' AS status
                ) AS source
                ON target.batch_id = source.batch_id
                AND target.source_name = source.source_name

                WHEN MATCHED THEN
                    UPDATE SET
                        processed_at = source.processed_at,
                        source_name = source.source_name,
                        status = source.status,
                        run_id = source.run_id

                WHEN NOT MATCHED THEN
                    INSERT (
                        run_id,
                        batch_id,
                        source_name,
                        bronze_rows_read,
                        after_business_validation,
                        deduplicated_rows,
                        inserted_rows,
                        updated_rows,
                        stale_rows,
                        late_rows,
                        quarantined_rows,
                        processed_at,
                        status,
                        processing_time_seconds,
                        last_completed_phase
                    )
                    VALUES (
                        source.run_id, source.batch_id, source.source_name,
                        NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                        source.processed_at,
                        source.status,NULL,NULL
                    )

                """)

        log_event(batch_id, "BATCH_INIT", "Batch marked as STARTED.")
        print("Batch state initialized.")



        ## PHASE 4 — Read Bronze Data for Selected Batch (Strict Schema Enforcement)
        if last_phase is None or last_phase < 4:

            # Step 1 — Read Bronze Data for This Batch
            bronze_df = (
                spark.table(f"project_1.bronze.{table_name}")
                .filter(col("_batch_id") == batch_id)
            )

            # Step 2 — Parse dates of Bronze Data
            silver_input_parsed_df = (
                bronze_df
                .withColumn(
                    "parsed_date", 
                    coalesce(
                        to_timestamp(col("updated_at")),
                        try_to_timestamp(col("updated_at"), lit("dd/MM/yyyy")),
                        try_to_timestamp(col("updated_at"), lit("MM-dd-yyyy")),
                        try_to_timestamp(col("updated_at"), lit("yyyy/MM/dd")),
                        try_to_timestamp(col("updated_at"), lit("yyyy-MM-dd"))
                    )
                )
                .withColumn("_silver_updated_ts", current_timestamp())
            )

            # Step 3 - Label Quarantine to Invalid Records
            
            silver_input_df = (
                silver_input_parsed_df
                .withColumn(
                    "quarantine_reason",
                    when(col("parsed_date").isNull(), "invalid date format")
                    .when((col("parsed_date").isNotNull()) & (col("parsed_date") > col("_ingestion_ts")), "updated_at is in future")
                    .when((col("phone").isNotNull()) & (regexp_extract(col("phone"), "^[0-9]+$", 0) == ""), "non-digit phone number")
                    .when(col("city").isNull(), "city is null")
                    .when(trim(col("city")) == "", "city is empty")
                )
            )
            
            #checkpoint temp_table
            silver_input_df = silver_input_df.filter(col("_batch_id") == batch_id)

            silver_input_df\
            .write\
            .mode("overwrite")\
            .option("replaceWhere", f"_batch_id = '{batch_id}'")\
            .saveAsTable(f"project_1.temp.{table_name}_silver_input_df")

            log_event(batch_id, "BRONZE_PARSED", "Bronze data parsed.")
            
            update_phase(batch_id, "PHASE_4")
        
        else:
            silver_input_df = (
                spark.table(f"project_1.temp.{table_name}_silver_input_df")
                .filter(col("_batch_id") == batch_id)
                )

        
        ## PHASE 5 — Business Validation + Quarantine Split

        if last_phase is None or last_phase < 5:

            validated_df = (
                silver_input_df
                .select(
                    col("customer_id"),
                    col("customer_name"),
                    col("city"),
                    col("phone"),
                    col("updated_at").alias("bronze_updated_at"),
                    col("parsed_date").alias("updated_at"),
                    col("_batch_id").alias("_bronze_batch_id"),
                    col("_ingestion_ts").alias("_bronze_ingestion_ts"),
                    col("_silver_updated_ts"),
                    col("quarantine_reason")
                )
            )

            # Step 1 — Split Valid vs Invalid
            valid_df = validated_df.filter(col("quarantine_reason").isNull()).drop(col("bronze_updated_at"))
            invalid_df = validated_df.filter(col("quarantine_reason").isNotNull())

            # checkpoint temp_table
            valid_df = valid_df.filter(col("_bronze_batch_id") == batch_id)

            valid_df\
            .write\
            .mode("overwrite")\
            .option("replaceWhere", f"_bronze_batch_id = '{batch_id}'")\
            .saveAsTable(f"project_1.temp.{table_name}_silver_valid_df")


            # Step 2 — Capture Counts & update bronze_metrics table
            metrics_df = (
                validated_df
                .agg(
                    count("*").alias("bronze_count"),
                    sum(when(col("quarantine_reason").isNull(), 1).otherwise(0)).alias("validated_count"),
                    sum(when(col("quarantine_reason").isNotNull(), 1).otherwise(0)).alias("quarantined_count")
                ).collect()[0]
            )

            after_validation_count = metrics_df["validated_count"]
            quarantined_count = metrics_df["quarantined_count"]
            bronze_count = metrics_df["bronze_count"]

            spark.sql(f"""
                    UPDATE project_1.monitoring.silver_batch_metrics
                    SET 
                        bronze_rows_read = {bronze_count},
                        after_business_validation = {after_validation_count},
                        quarantined_rows = {quarantined_count}
                    WHERE batch_id = '{batch_id}'
                    AND source_name = '{table_name}'
                    """)
            log_event(batch_id, "READ_BRONZE", f"Bronze rows read: {bronze_count}")
            log_event(batch_id, "VALIDATION", f"Valid rows: {after_validation_count}")

            # Step 3 — Enforce Threshold Rule
            invalid_pct = 0

            if bronze_count > 0:
                invalid_pct = (quarantined_count / bronze_count) * 100
            print(f"Invalid percentage: {invalid_pct:.2f}%")

            if invalid_pct > threshold:
                spark.sql(f"""
                        UPDATE project_1.monitoring.silver_batch_metrics
                        SET
                            status = 'FAILED'
                            WHERE batch_id = '{batch_id}'
                            AND source_name = '{table_name}'
                        """)
                log_event(batch_id, "THRESHOLD_CHECK", f"Invalid percentage {invalid_pct:.2f}% exceeded threshold {threshold}%", level="ERROR")
                raise Exception("Invalid threshold exceeded. Batch marked as FAILED.")

            # Step 5 — Write Quarantine Table
            (
                invalid_df
                .withColumn("_silver_quarantined_ts", current_timestamp())
                .select(
                    col("customer_id"),
                    col("customer_name"),
                    col("city"),
                    col("phone"),
                    col("bronze_updated_at").try_cast("timestamp"),
                    col("quarantine_reason"),
                    col("_bronze_batch_id"),
                    col("_bronze_ingestion_ts"),
                    col("_silver_quarantined_ts")
                )
                .write
                .mode("append")
                .insertInto(f"project_1.silver.{table_name}_quarantine")
            )

            log_event(batch_id, "QUARANTINE_WRITE", f"Quarantined rows written: {quarantined_count}")

            update_phase(batch_id, "PHASE_5")

        else:
            valid_df = (
                spark.table(f"project_1.temp.{table_name}_silver_valid_df")
                .filter(col("_bronze_batch_id") == batch_id)
                )


        
        ## PHASE 6 — Deduplication (Correct Way)
        
        if last_phase is None or last_phase < 6:
            window_spec = Window.partitionBy("customer_id")\
                                .orderBy(col("updated_at").desc(), col("_bronze_ingestion_ts").desc())

            dedup_df = (
                valid_df
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop("rn", "quarantine_reason")
            )

            #checkpoint temp table
            dedup_df = dedup_df.filter(col("_bronze_batch_id") == batch_id)

            dedup_df\
            .write\
            .mode("overwrite")\
            .option("replaceWhere", f"_bronze_batch_id = '{batch_id}'")\
            .insertInto(f"project_1.temp.{table_name}_silver_dedup_df")

            dedup_count = dedup_df.count()

            spark.sql(f"""
                        UPDATE project_1.monitoring.silver_batch_metrics
                        SET
                        deduplicated_rows = {dedup_count}
                        WHERE batch_id = '{batch_id}' 
                        AND source_name = '{table_name}'        
                        """)

            log_event(batch_id, "DEDUPLICATION", f"Rows after deduplication: {dedup_count}")
            
            update_phase(batch_id, "PHASE_6")
        else:
            dedup_df = (
                spark.table(f"project_1.temp.{table_name}_silver_dedup_df")
                .filter(col("_bronze_batch_id") == batch_id)
                )


        
        ## PHASE 7 — Capture Insert / Update / Stale Metrics
        if last_phase is None or last_phase < 7:
            # Step 1 — Join With Existing Silver
            silver_df = spark.table(f"project_1.silver.{table_name}")

            merge_analysis = (
                dedup_df.alias("s")
                .join(
                    silver_df.alias("t"),
                    "customer_id",
                    "left"
                )
            )

            # Step 2 — Classify Rows
            analysis_df = (
                merge_analysis
                .withColumn(
                    "operation",
                    when(col("t.customer_id").isNull(), "INSERT")
                    .when(col("s.updated_at") > col("t.updated_at"), "UPDATE")
                    .when(col("s.updated_at") < col("t.updated_at"), "LATE")
                    .otherwise("STALE")
                )
            )

            # Step 3 — Compute Metrics
            metrics = (
                analysis_df
                .groupBy("operation")
                .count()
                .collect()
            )

            inserted = 0
            updated = 0
            stale = 0
            late = 0

            for row in metrics:
                if row["operation"] == "INSERT":
                    inserted = row["count"]
                elif row["operation"] == "UPDATE":
                    updated = row["count"]
                elif row["operation"] == "LATE":
                    late = row["count"]
                elif row["operation"] == "STALE":
                    stale = row["count"]

            # Step 4 — Update Batch Metrics
            spark.sql(f"""
                    UPDATE project_1.monitoring.silver_batch_metrics
                    SET
                        inserted_rows = {inserted},
                        updated_rows = {updated},
                        stale_rows = {stale},
                        late_rows = {late}
                    WHERE batch_id = '{batch_id}'
                    AND source_name = '{table_name}'
                    """)

            log_event(batch_id, "METRICS_WRITE", f"Metrics written: {inserted} INSERTED, {updated} UPDATED, {stale} STALE")
            
            update_phase(batch_id, "PHASE_7")
        else:
            dedup_df = (
                spark.table(f"project_1.temp.{table_name}_silver_dedup_df")
                .filter(col("_bronze_batch_id") == batch_id)
                )

        
        ## PHASE 8 — SCD Type 1 MERGE (Production Version)
        if last_phase is None or last_phase < 8:
            # Step 1 — Prepare Merge Data (add the Silver metadata columns.)
            merge_df = (
                dedup_df
                .withColumn("_silver_updated_ts", current_timestamp())
            )

            # Step 2 — Load Delta Table
            silver_table = DeltaTable.forName(spark, f"project_1.silver.{table_name}")

            # Step 3 — Perform SCD1 Merge
            (
                silver_table.alias("t")
                .merge(
                    merge_df.alias("s"), "t.customer_id = s.customer_id" 
                )

                .whenMatchedUpdate(
                    condition = "s.updated_at > t.updated_at",
                    set = {
                        "customer_id": "s.customer_id",
                        "customer_name": "s.customer_name",
                        "city": "s.city",
                        "phone": "s.phone",
                        "updated_at": "s.updated_at",
                        "_bronze_batch_id": "s._bronze_batch_id",
                        "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
                        "_silver_updated_ts": "s._silver_updated_ts"
                    }
                )

                .whenNotMatchedInsert(
                    values = {
                        "customer_id": "s.customer_id",
                        "customer_name": "s.customer_name",
                        "city": "s.city",
                        "phone": "s.phone",
                        "updated_at": "s.updated_at",
                        "_bronze_batch_id": "s._bronze_batch_id",
                        "_bronze_ingestion_ts": "s._bronze_ingestion_ts",
                        "_silver_updated_ts": "s._silver_updated_ts"
                    }
                )

                .execute()
            )

            log_event(batch_id, "MERGE_SCD1", "Merge completed")

            update_phase(batch_id, "PHASE_8")

        
        ## PHASE 9 — Logging + Completion
        if last_phase is None or last_phase < 9:
            spark.sql(f"""
                        UPDATE project_1.monitoring.silver_batch_metrics
                        SET
                        status = 'SUCCESS',
                        processing_time_seconds = 
                        unix_timestamp(current_timestamp()) - unix_timestamp(processed_at)
                        WHERE batch_id = '{batch_id}'
                        AND source_name = '{table_name}'
                        """)

            log_event(batch_id, "PIPELINE_COMPLETE", "Batch processed successfully.")
            print(f"Batch id:- {batch_id} processed successfully.")

            update_phase(batch_id, "PHASE_9")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")


process_batch(batch_id)