# Pipeline Architecture

# Overview

This project implements an end-to-end Medallion Architecture-based Data Engineering Pipeline using FastAPI, Databricks, PySpark, Delta-style processing concepts, and operational monitoring frameworks.

The pipeline simulates a production-inspired food delivery platform processing architecture with automated ingestion, layered transformations, monitoring, orchestration, and dashboard reporting.

---

# High-Level Architecture

```text
Local Data Generator
        ↓
CSV File Generation
        ↓
FastAPI Service
        ↓
ngrok Public Endpoint
        ↓
Databricks API Ingestion
        ↓
Raw Volume Storage (Parquet)
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Gold Layer
        ↓
Dashboards & Monitoring
```

---

# Core Architecture Principles

The pipeline follows several modern Data Engineering principles:

- Medallion Architecture
- Incremental Processing
- Idempotent Ingestion
- Data Quality Validation
- Operational Monitoring
- Observability
- Failure Isolation
- Modular Orchestration
- Recovery-Oriented Design

---

# Source Layer

## Data Generator

The source layer uses a Python-based synthetic data generator to simulate food delivery operational datasets.

Generated datasets include:

- customers
- orders
- order_items
- restaurants
- delivery_partners

The generator supports:
- incremental file generation
- partition-based datasets
- randomized operational behavior
- historical state management

---

## FastAPI Service

The FastAPI service exposes generated CSV files through REST APIs.

Primary responsibilities:

- file discovery APIs
- date-based dataset retrieval
- authentication validation
- source availability exposure

Example APIs:

```text
/files
/orders?date=2026-02-01
/customers?date=2026-02-01
```

---

# Ingestion Layer

## API Ingestion Framework

Databricks notebooks consume source files using authenticated API requests.

Core ingestion capabilities:

- dynamic file discovery
- idempotent ingestion
- partition-aware loading
- ingestion auditing
- error handling
- missing file detection
- operational alerting

---

## Raw Storage

Raw API datasets are stored as parquet files in Databricks Volumes.

Example structure:

```text
/Volumes/project_1/raw_dump/orders/date=2026-02-01/
```

The raw layer acts as:
- landing zone
- replay source
- recovery layer
- immutable ingestion checkpoint

---

# Bronze Layer

The Bronze layer standardizes and validates raw source datasets.

Primary responsibilities:

- schema standardization
- metadata enrichment
- ingestion auditing
- duplicate handling
- partition management
- operational metrics generation

Each source has an independent Bronze processing notebook.

---

# Silver Layer

The Silver layer performs business-level cleansing and incremental processing.

Primary responsibilities:

- deduplication
- validation checks
- quarantine handling
- merge/upsert processing
- late-arriving data handling
- stale data detection
- incremental batch tracking

Silver processing introduces curated analytical quality datasets.

---

# Gold Layer

The Gold layer creates aggregated business reporting tables.

Primary responsibilities:

- KPI aggregation
- dimensional reporting
- operational summaries
- trend calculations
- analytical serving tables

Example outputs:

- daily business summary
- restaurant performance metrics
- customer activity summaries

---

# Monitoring Framework

The project includes a dedicated operational monitoring framework.

Monitoring coverage includes:

- Bronze ingestion health
- Silver batch health
- invalid record tracking
- processing throughput
- batch observability
- silent failure detection
- recovery visibility

Monitoring data is stored in dedicated monitoring schemas.

---

# Alerting Framework

Slack-based operational alerts are integrated throughout orchestration stages.

Alert categories include:

- ingestion failures
- partial success
- missing files
- empty datasets
- orchestration anomalies
- pipeline completion summaries

---

# Orchestration Framework

Databricks Workflows orchestrate the pipeline execution.

Execution flow:

```text
API Ingestion
    ↓
Bronze Orchestration
    ↓
Silver Orchestration
    ↓
Gold Aggregation
    ↓
Dashboards
    ↓
Orchestration Alerts
```

Dependency-based execution ensures:
- upstream validation
- controlled failures
- compute optimization
- operational visibility

---

# Dashboard Architecture

The project includes three primary dashboards:

## Executive Overview Dashboard

Focus Areas:
- revenue trends
- order trends
- operational KPIs

---

## Restaurant Analytics Dashboard

Focus Areas:
- restaurant performance
- completion rates
- operational rankings

---

## Pipeline Monitoring Dashboard

Focus Areas:
- ingestion health
- batch quality
- processing performance
- operational observability

---

# Data Quality Framework

The pipeline implements multiple data quality controls.

Examples include:

- schema validation
- null handling
- duplicate detection
- invalid record isolation
- stale data detection
- late-arriving data tracking

Invalid or suspicious records are isolated for operational review.

---

# Idempotency Design

The ingestion framework supports idempotent processing using:

- ingestion tracking tables
- partition-aware checks
- previously processed file detection

This prevents duplicate ingestion during reruns or retries.

---

# Recovery Strategy

The architecture supports recovery-oriented operations through:

- raw parquet persistence
- audit tables
- operational metrics
- batch-level tracking
- isolated orchestration layers

This enables:
- replayability
- debugging
- failure isolation
- operational troubleshooting

---

# Scalability Considerations

The architecture was designed with scalability principles including:

- modular notebook design
- partition-based processing
- layered transformations
- orchestration separation
- monitoring isolation
- reporting abstraction

---

# Future Enhancements

Potential future improvements include:

- Delta Lake implementation
- Change Data Capture (CDC)
- Structured Streaming
- Auto Loader
- CI/CD integration
- Infrastructure as Code
- Airflow orchestration
- cloud-hosted APIs
- containerized deployment
- role-based access control