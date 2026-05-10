# Databricks notebook source
# DBTITLE 1,Import alerts
# MAGIC %run
# MAGIC <your_alerts_notebook_path>

# COMMAND ----------

# DBTITLE 1,API Ingestion
# STEP 1 — GET AVAILABLE FILES
import requests
import pandas as pd
from io import BytesIO

BASE_URL = "<https://ngroklink>"   # or localhost
HEADERS = {"Authorization": "Bearer <api_authorization_code>"}

source_files = ["customers", "order_items", "orders", "restaurants", "delivery_partners"]

api_results = {}

# 🔥 CALL FILE DISCOVERY API
response = requests.get(f"{BASE_URL}/files", headers=HEADERS)

# Check status before parsing JSON
if response.status_code != 200:
    raise Exception(
        f"Failed to fetch file list from API. "
        f"HTTP {response.status_code}: {response.text[:200]}"
    )


file_map = response.json()

# STEP 2 — LOOP DYNAMICALLY
for source in source_files:
    
    # If source not present in API → treat as missing
    if source not in file_map:
        api_results[f"{source}_NA"] = {
            "status": "MISSING",
            "reason": "No files returned by API"
        }
        continue

    for date in file_map[source]:
        # STEP 3 — IDEMPOTENCY FIRST (IMPORTANT)

        output_path = f"/Volumes/project_1/raw_dump/{source}/date={date}"

        try:
            existing = spark.sql(f"""
                    SELECT 1
                    FROM project_1.monitoring.api_ingestion_tracker
                    WHERE source_name = '{source}'
                    AND file_date = '{date}'
                    AND ingestion_status = 'SUCCESS'
                    LIMIT 1
                    """).count()

            if existing > 0:
                api_results[f"{source}_{date}"] = {
                    "status": "SKIPPED",
                    "reason": "Already ingested (tracker table)"
                }

                continue

        except Exception as e:
            if "FileNotFoundException" in str(e) or "java.io.FileNotFoundException" in str(e):
                # Expected → path doesn't exist → safe to write
                pass
            else:
                # Unexpected → log it properly
                raise e

        # STEP 4 — CALL API
        try:
            url = f"{BASE_URL}/{source}?date={date}"
            response = requests.get(url, headers=HEADERS)

            if response.status_code != 200:
                api_results[f"{source}_{date}"] = {
                    "status": "FAILED",
                    "reason": f"HTTP {response.status_code}"
                }
                continue
        
            # STEP 5 — READ CSV
            csv_io = BytesIO(response.content)

            try:
                pandas_df = pd.read_csv(csv_io)
            except Exception as e:
                api_results[f"{source}_{date}"] = {
                    "status": "FAILED",
                    "reason": str(e)
                }
                continue

            if pandas_df.empty:
                api_results[f"{source}_{date}"] = {
                    "status": "EMPTY",
                    "rows": 0
                }
                continue

            # STEP 6 — WRITE TO VOLUME
            spark_df = spark.createDataFrame(pandas_df)

            spark_df.write.mode("append").parquet(output_path)

            print(f"File found for {source} on {date}. Writing {len(pandas_df)} rows to {output_path}")


            # STEP 7 — VERIFY WRITE
            files = dbutils.fs.ls(output_path)

            if len(files) > 0:
                api_results[f"{source}_{date}"] = {
                    "status": "SUCCESS",
                    "rows": len(pandas_df)
                }
            else:
                api_results[f"{source}_{date}"] = {
                    "status": "FAILED",
                    "reason": "Write failed"
                }
                
            spark.sql(f"""
                INSERT INTO project_1.monitoring.api_ingestion_tracker
                VALUES (
                    '{source}',
                    DATE('{date}'),
                    'SUCCESS',
                    {len(pandas_df)},
                    '{output_path}',
                    current_timestamp()
                )
                """)

        # STEP 8 — EXCEPTION HANDLING
        except Exception as e:
            api_results[f"{source}_{date}"] = {
                "status": "FAILED",
                "reason": str(e)
            }

processed_results = {
    k:v for k,v in api_results.items()
    if v["status"] in ["SUCCESS", "FAILED", "EMPTY"]
}

# Alert Logic
success = [k for k,v in processed_results.items() if v["status"] == "SUCCESS"]
failed = [k for k,v in processed_results.items() if v["status"] == "FAILED"]
empty = [k for k,v in processed_results.items() if v["status"] == "EMPTY"]
skipped = [k for k,v in api_results.items() if v["status"] == "SKIPPED"]

missing_files = []

for source in source_files:
    expected_dates = file_map.get(source, [])
    
    processed_dates = [
        k.rsplit("_", 1)[1] for k in api_results.keys() if k.startswith(source)
    ]
    
    missing_dates = set(expected_dates) - set(processed_dates)
    
    for d in missing_dates:
        missing_files.append(f"{source}_{d}")


# Helper to format lists (clean Slack output)
def format_list(items, extra_info=None):
    if not items:
        return "- None"
    
    output = ""
    for i in items:
        if extra_info and i in extra_info:
            output += f"- {i} ({extra_info[i]})\n"
        else:
            output += f"- {i}\n"
    return output

total = len(processed_results)

success_cnt = len(success)
failed_cnt = len(failed)
skipped_cnt = len(skipped)
empty_cnt = len(empty)
missing_cnt = len(missing_files)

if success_cnt == total:
    overall_status = "ALL_SUCCESS"

elif success_cnt == 0 and skipped_cnt == total:
    overall_status = "NO_NEW_DATA"

elif failed_cnt == total:
    overall_status = "FAILED"

elif success_cnt > 0 and (failed_cnt > 0 or empty_cnt > 0 or missing_cnt > 0):
    overall_status = "PARTIAL_SUCCESS"

elif missing_cnt > 0:
    overall_status = "DATA_ISSUE"

else:
    overall_status = "PARTIAL"


# Dynamic Message

success_list = format_list(success, {k: f"{processed_results[k]['rows']} rows" for k in success})
failed_list = format_list(failed, {k: processed_results[k]['reason'] for k in failed})
skipped_list = format_list(skipped)
missing_list = format_list(missing_files)
empty_list = format_list(empty)


severity_map = {
    "ALL_SUCCESS": "INFO",
    "NO_NEW_DATA": "INFO",
    "PARTIAL_SUCCESS": "WARNING",
    "PARTIAL": "CRITICAL",
    "DATA_ISSUE": "CRITICAL",
    "FAILED": "CRITICAL"
}


# Alert Message
# ================================
# MESSAGE TEMPLATES
# ================================

if overall_status == "ALL_SUCCESS":

    message = f"""📡 SOURCE API ALERT

    File Date: {date}
    Status: ✅ ALL SUCCESS
    Severity: {severity_map[overall_status]}

    ================ SUMMARY =================
    Total Sources: {total}
    ❌ Failed: {failed_cnt}
    🚫 Missing: {missing_cnt}
    ⏭️ Skipped: {skipped_cnt}
    ✅ Success: {success_cnt}
    ⚠️ Empty: {empty_cnt}

    ================ DETAILS =================
    ✅ INGESTED:
    {success_list}

    🎯 All sources ingested successfully. Pipeline will proceed.
    """


elif overall_status == "NO_NEW_DATA":

    message = f"""📡 SOURCE API ALERT

    File Date: {date}
    Status: ⏭️ NO NEW DATA
    Severity: {severity_map[overall_status]}


    ================ SUMMARY =================
    Total Sources: {total}
    ❌ Failed: {failed_cnt}
    🚫 Missing: {missing_cnt}
    ⏭️ Skipped: {skipped_cnt}
    ✅ Success: {success_cnt}
    ⚠️ Empty: {empty_cnt}

    ================ DETAILS =================
    ⏭️ SKIPPED:
    {skipped_list}

    ℹ️ No new data to ingest. Pipeline will not proceed.
    """


elif overall_status == "PARTIAL_SUCCESS":

    message = f"""📡 SOURCE API ALERT

    File Date: {date}
    Status: ⚠️ PARTIAL SUCCESS
    Severity: {severity_map[overall_status]}


    ================ SUMMARY =================
    Total Sources: {total}
    ❌ Failed: {failed_cnt}
    🚫 Missing: {missing_cnt}
    ⏭️ Skipped: {skipped_cnt}
    ✅ Success: {success_cnt}
    ⚠️ Empty: {empty_cnt}

    ================ DETAILS =================
    """

    if success:
        message += f"\n✅ INGESTED:\n{success_list}\n"

    if skipped:
        message += f"\n⏭️ SKIPPED:\n{skipped_list}\n"

    if empty:
        message += f"\n⚠️ EMPTY DATA:\n{empty_list}\n"

    if failed:
        message += f"\n❌ FAILED:\n{failed_list}\n"

    if missing_files:
        message += f"\n🚫 MISSING:\n{missing_list}\n"

    message += "\n⚠️ Some sources failed. Pipeline will NOT proceed.\n"


elif overall_status == "DATA_ISSUE":

    message = f"""📡 SOURCE API ALERT

    File Date: {date}
    Status: 🚨 DATA ISSUE
    Severity: {severity_map[overall_status]}


    ================ SUMMARY =================
    Total Sources: {total}
    ❌ Failed: {failed_cnt}
    🚫 Missing: {missing_cnt}
    ⏭️ Skipped: {skipped_cnt}
    ✅ Success: {success_cnt}
    ⚠️ Empty: {empty_cnt}

    ================ DETAILS =================
    🚫 MISSING:
    {missing_list}

    ⏭️ SKIPPED:
    {skipped_list}

    ⚠️ EMPTY DATA:
    {empty_list}

    🚨 ACTION REQUIRED:
    Some expected files were not generated. Pipeline will NOT proceed.
    """


elif overall_status == "FAILED":

    message = f"""📡 SOURCE API ALERT

    File Date: {date}
    Status: ❌ FAILED
    Severity: {severity_map[overall_status]}


    ================ SUMMARY =================
    Total Sources: {total}
    ❌ Failed: {failed_cnt}
    🚫 Missing: {missing_cnt}
    ⏭️ Skipped: {skipped_cnt}
    ✅ Success: {success_cnt}
    ⚠️ Empty: {empty_cnt}

    ================ DETAILS =================
    ❌ FAILED:
    {failed_list}

    🚫 MISSING:
    {missing_list}

    🚨 ACTION REQUIRED:
    Critical failure in source ingestion. Pipeline stopped.
    """
elif overall_status == "PARTIAL":

    message = f"""📡 SOURCE API ALERT

    File Date: {date}
    Status: ⚠️ PARTIAL (WITH FAILURES)
    Severity: {severity_map[overall_status]}


    ================ SUMMARY =================
    Total Sources: {total}
    ✅ Success: {success_cnt}
    ⏭️ Skipped: {skipped_cnt}
    ⚠️ Empty: {empty_cnt}
    ❌ Failed: {failed_cnt}
    🚫 Missing: {missing_cnt}

    ================ DETAILS =================
    ❌ FAILED:
    {failed_list}

    ✅ SUCCESS:
    {success_list}

    ⏭️ SKIPPED:
    {skipped_list}

    ⚠️ EMPTY:
    {empty_list}

    ⚠️ Partial ingestion with failures. Pipeline will NOT proceed.
    """

# COMMAND ----------

# DBTITLE 1,SLACK ALERT
send_alert(message, "INFO" if overall_status=="SUCCESS" else "ERROR")


if overall_status == "ALL_SUCCESS":
    dbutils.notebook.exit("SUCCESS")
elif overall_status == "PARTIAL_SUCCESS" or overall_status == "PARTIAL":
    raise Exception("PARTIAL_SUCCESS - stopping pipeline")
elif overall_status == "NO_NEW_DATA":
    raise Exception("NO_NEW_DATA - stopping pipeline")
else:
    raise Exception("FAILED - stopping pipeline")

# COMMAND ----------

