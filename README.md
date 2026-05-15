# End-to-End Medallion Data Engineering Pipeline using Databricks

Production-inspired batch Data Engineering pipeline built using Databricks, PySpark, Delta Lake, FastAPI, SQL, and Databricks Workflows.

The project simulates a real-world food delivery platform where operational datasets are generated daily, exposed through APIs, incrementally ingested into Databricks, validated across Bronze/Silver/Gold layers, monitored through audit frameworks, and transformed into business-ready KPI dashboards.

The pipeline was intentionally designed with strong focus on operational reliability, observability, monitoring, recovery-safe orchestration, and production-style engineering practices instead of only ETL transformations.

---

## Key Engineering Highlights

* API-driven ingestion using FastAPI
* Bronze / Silver / Gold Medallion Architecture
* Incremental and idempotent ingestion framework
* Schema enforcement and schema drift handling
* Validation and quarantine framework
* Threshold-based batch failure handling
* Incremental merge processing using Delta Lake
* Silent failure tracking and operational monitoring
* Recovery-safe orchestration using Databricks Jobs
* Slack-based operational alerting
* Business KPI dashboards using Databricks Dashboards

---

## Architecture Overview

```text
Data Generator Scripts
        ↓
FastAPI Layer
        ↓
API Ingestion Notebook
(dynamic file discovery + idempotent ingestion)
        ↓
Databricks Volumes (Raw Parquet Storage)
        ↓
Bronze Layer
(raw ingestion + schema enforcement + audit logging)
        ↓
Silver Layer
(validation + quarantine + deduplication + merge logic)
        ↓
Gold Layer
(business KPIs + reporting tables)
        ↓
Dashboards & Monitoring
```

![Architecture Diagram](assets/architecture_diagram.png)

---

## Tech Stack

| Category         | Technologies                 |
| ---------------- | ---------------------------- |
| Data Platform    | Databricks, Delta Lake       |
| Processing       | PySpark, SQL                 |
| Backend/API      | Python, FastAPI              |
| Storage          | Databricks Volumes, Parquet  |
| Orchestration    | Databricks Workflows         |
| Monitoring       | Audit Tables, Slack Alerts   |
| Architecture     | Medallion Architecture       |
| Processing Style | Incremental Batch Processing |

---

## Pipeline Workflow

```text
1. Python scripts generate operational CSV datasets
        ↓
2. FastAPI exposes generated datasets through REST APIs
        ↓
3. API ingestion notebook dynamically discovers new files
        ↓
4. Files are ingested into Databricks Volumes as parquet
        ↓
5. Bronze layer standardizes raw ingestion and audit tracking
        ↓
6. Silver layer applies validations and merge processing
        ↓
7. Invalid records are quarantined
        ↓
8. Gold layer generates business KPIs
        ↓
9. Dashboards and monitoring tables are refreshed
        ↓
10. Slack alerts summarize operational pipeline status
```

---

## Medallion Layer Responsibilities

| Layer         | Responsibility                                              |
| ------------- | ----------------------------------------------------------- |
| API Ingestion | Dynamic API-driven file ingestion                           |
| Bronze        | Raw ingestion, schema standardization, audit tracking       |
| Silver        | Validation, quarantine, deduplication, merge processing     |
| Gold          | Business KPI aggregation and reporting                      |
| Monitoring    | Alerting, orchestration tracking, operational observability |

---

## Key Features

| Feature                     | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| Dynamic API Ingestion       | Automatically discovers and ingests newly generated datasets |
| Idempotent Processing       | Prevents duplicate ingestion during reruns                   |
| Schema Drift Handling       | Detects evolving source schemas safely                       |
| Quarantine Framework        | Invalid records isolated with failure reasons                |
| Incremental Merge Logic     | Controlled upsert processing in Silver layer                 |
| Monitoring Framework        | Tracks pipeline execution and operational health             |
| Silent Failure Tracking     | Detects partially failed datasets                            |
| Recovery-Safe Orchestration | Prevents downstream cascading failures                       |
| Slack Alerting              | Sends operational execution notifications                    |
| Dashboard Reporting         | Generates business-ready KPI reporting tables                |

---

## Dashboard & Reporting

The Gold layer generates curated analytical datasets for operational reporting and KPI monitoring.

### Business KPIs

* Total Orders
* Completed Orders
* Failed Orders
* Revenue Trends
* Average Order Value
* Restaurant Performance
* Item-Level Analytics
* Operational Order Trends

### Dashboards Implemented

* Executive Overview Dashboard
* Restaurant Analytics Dashboard
* Pipeline Monitoring Dashboard

> Dashboard screenshots available in the `assets/dashboard_screenshots/` folder.

---

## Repository Structure

```text
project-root/
│
├── README.md
├── requirements.txt
│
├── docs/
├── assets/
├── scripts/
├── notebooks/
├── monitoring/
└── dashboards/
```

---

## Documentation

Detailed technical documentation is available inside the `docs/` directory.

| Document              | Description                                 |
| --------------------- | ------------------------------------------- |
| architecture.md       | Complete system architecture explanation    |
| pipeline_setup.md     | Full project setup guide                    |
| bronze_layer.md       | Bronze layer processing logic               |
| silver_layer.md       | Validation, quarantine, and merge framework |
| gold_layer.md         | KPI aggregation and reporting logic         |
| orchestration.md      | Databricks workflow orchestration           |
| monitoring_alerts.md  | Monitoring and alerting framework           |
| recovery_debugging.md | Recovery and debugging workflows            |

---

## Monitoring & Operational Reliability

The project was intentionally designed with strong operational controls including:

* Batch-level audit logging
* Validation monitoring
* Silent failure tracking
* Threshold-based batch stopping
* Recovery-safe orchestration
* Downstream dependency control
* Slack-based operational alerts
* Manual recovery support

---

## Future Improvements

Planned future enhancements include:

* Streaming ingestion workflows
* Airflow-based orchestration
* CI/CD integration
* Infrastructure as Code using Terraform
* Great Expectations integration
* Advanced observability dashboards
* Cloud-native deployment
* Automated recovery workflows

---

## Key Learning Outcomes

This project provided hands-on exposure to:

* Medallion Architecture implementation
* Incremental data processing
* API-driven ingestion workflows
* PySpark transformation patterns
* Delta Lake merge strategies
* Monitoring and observability concepts
* Operational pipeline design
* Recovery-safe orchestration
* Data quality and validation frameworks
* Business KPI aggregation

---

## Dashboard Preview

> Dashboard screenshots and workflow images are available in the `assets/` directory.

![Pipeline Flow](assets/pipeline_flow.png)
