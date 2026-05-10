# Pipeline Setup Guide

This document explains how to set up and execute the end-to-end Medallion Data Engineering Pipeline locally using FastAPI, Databricks, and Databricks Workflows.

---

# 1. Local Project Structure

Create a local project directory.

Example:

```text
/users/johndoe/etl_project/
```

Recommended folder structure:

```text
etl_project/
│
├── fastapi/
│   └── fastapi_service.py
│
├── data/
│   └── <generated CSV files>
│
├── data_generation/
│   ├── data_generator.py
│   ├── config.py
│   └── state/
│       └── <master tracking files>
```

---

# 2. Data Generation Setup

The pipeline uses a Python-based synthetic data generator to simulate operational food delivery datasets.

## Run Data Generator Manually

Use terminal:

```bash
python /users/johndoe/etl_project/data_generation/data_generator.py
```

or

```bash
python3 /users/johndoe/etl_project/data_generation/data_generator.py
```

---

## Optional: Automate Using Cron Job

You may configure a cron job to generate datasets automatically at scheduled intervals.

Example:

```bash
0 8 * * * python3 /users/johndoe/etl_project/data_generation/data_generator.py
```

This example runs the generator daily at 8:00 AM.

---

# 3. FastAPI Setup

The FastAPI service exposes generated CSV files through REST APIs for Databricks ingestion.

---

## Install Dependencies

Install FastAPI and Uvicorn:

```bash
pip install fastapi uvicorn
```

---

## Start FastAPI Server

Navigate to the FastAPI project directory and run:

```bash
uvicorn main:app --reload
```

The API should now run locally on:

```text
http://127.0.0.1:8000
```

Keep this terminal session active.

---

# 4. ngrok Setup

ngrok is used to expose the local FastAPI server publicly so Databricks can access the APIs.

---

## Install ngrok

MacOS example:

```bash
brew install ngrok/ngrok/ngrok
```

---

## Configure ngrok

Create a free account at:

```text
https://ngrok.com
```

Copy your authentication token and run:

```bash
ngrok config add-authtoken <YOUR_AUTH_TOKEN>
```

---

## Start ngrok Tunnel

Run:

```bash
ngrok http 8000
```

You should now see a forwarding URL similar to:

```text
https://abc123.ngrok-free.app -> http://localhost:8000
```

This becomes the public API endpoint used by Databricks.

---

# 5. API Validation

Before integrating with Databricks, validate that the API can successfully return files.

Example:

```bash
curl -H "Authorization: Bearer johndoe123" \
"https://abc123.ngrok-free.app/orders?date=2026-02-01" \
--output test.csv
```

If the file downloads successfully, the API setup is complete.

---

# 6. Databricks Environment Setup

Create a Databricks Free Edition account or use an existing Databricks workspace.

---

# 7. Storage Setup

## Create Catalog

Navigate to Catalog and create a new catalog for the project.

Example:

```text
project_1
```

---

## Create Schemas

Create required schemas such as:

```text
bronze
silver
gold
monitoring
raw_dump
```

The `raw_dump` schema stores API-ingested raw parquet files.

---

# 8. Workspace Setup

Navigate to Workspace and create a dedicated project folder.

Example:

```text
Pipeline_Project_1
```

Recommended structure:

```text
Pipeline_Project_1/
│
├── api_ingestion/
├── bronze/
├── silver/
├── gold/
├── orchestration/
└── alerts/
```

This helps organize notebooks cleanly and improves maintainability.

---

# 9. Table Setup

The repository contains SQL files for creating required project tables.

Location:

```text
monitoring/audit_tables/
```

and

```text
notebooks/tables/
```

---

## Execute Table Creation Scripts

For each SQL file:

1. Create a Databricks notebook
2. Paste the SQL script
3. Execute the notebook

---

## Important Naming Convention

The catalog name must match your project catalog.

Example:

```sql
project_1.bronze.customers
```

Where:

- `project_1` = catalog
- `bronze` = schema
- `customers` = table

---

# 10. Slack Alert Framework Setup

Create a notebook for the Slack alert framework.

Copy the script from:

```text
notebooks/alerts/
```

Replace:
- webhook URLs
- tokens
- credentials

with valid values.

---

# 11. Main Notebook Setup

Export or copy notebooks from the GitHub project repository into their corresponding Databricks workspace folders.

Example:

```text
api_ingestion/
bronze/
silver/
gold/
orchestration/
alerts/
```

---

## Recommended Execution Order

1. API Ingestion
2. Bronze Orchestration
3. Silver Orchestration
4. Gold Aggregation
5. Dashboards
6. Orchestration Alerts

---

# 12. Dashboard Setup

Navigate to the Databricks Dashboards section.

---

## Create Dashboard

1. Create a new dashboard
2. Click "Data"
3. Add SQL Dataset
4. Paste SQL queries from:

```text
dashboards/
```

Example:

```text
executive_overview_queries.sql
```

---

## Create Visualizations

Use dashboard screenshots available under:

```text
assets/dashboard_screenshots/
```

as references for:
- KPI cards
- trend charts
- operational monitoring visuals

---

# 13. Databricks Workflow Setup

Databricks Jobs are used as the orchestration framework for the entire pipeline.

---

# Workflow Execution Order

```text
API_INGESTION
    ↓
BRONZE_ORCHESTRATION
    ↓
SILVER_ORCHESTRATION
    ↓
GOLD_AGGREGATION
    ↓
DASHBOARDS
    ↓
ORCHESTRATION_ALERTS
```

---

## Create Workflow

1. Navigate to:
   - Jobs & Pipelines
2. Click:
   - Create Job
3. Add notebook tasks sequentially

---

## Dependency Configuration

### API Ingestion
- No dependencies

### Bronze Orchestration
Depends on:
- API Ingestion

Condition:
```text
All Succeeded
```

---

### Silver Orchestration
Depends on:
- Bronze Orchestration

Condition:
```text
All Succeeded
```

---

### Gold Aggregation
Depends on:
- Silver Orchestration

Condition:
```text
All Succeeded
```

---

### Dashboards
Depends on:
- Gold Aggregation

Condition:
```text
All Done
```

---

### Orchestration Alerts
Depends on:
- Gold Aggregation

Condition:
```text
All Done
```

This ensures alerts and monitoring continue even if downstream reporting fails.

---

# 14. Operational Notes

## Important Considerations

- The FastAPI service and ngrok tunnel must remain active while Databricks jobs execute.
- If the local machine enters sleep mode, API connectivity may fail.
- Databricks job schedules should align with local machine availability when running locally.

---

# 15. Future Improvements

Potential future production enhancements include:

- Cloud-hosted APIs
- CI/CD integration
- Infrastructure as Code
- Airflow orchestration
- Secret management
- Real-time ingestion
- Containerized deployment
- Automated schema evolution