## Overview

The API ingestion layer is responsible for dynamically discovering newly generated source files, ingesting unprocessed datasets into Databricks Volumes, enforcing file-level idempotency, and generating operational alerts for ingestion visibility.

The ingestion framework was designed to simulate production-style API-driven batch ingestion workflows where upstream source systems expose datasets through REST endpoints instead of direct filesystem access.

The ingestion process dynamically identifies newly available files through FastAPI endpoints and ingests only datasets that have not been previously processed.

## Ingestion Workflow

```text
Python Data Generator
        ↓
Local CSV File Generation
        ↓
FastAPI Service
        ↓
Dynamic File Discovery API (/files)
        ↓
API Ingestion Notebook
        ↓
File-Level Idempotency Check
        ↓
Parquet Write to Databricks Volumes
        ↓
Monitoring & Alert Generation
```

## Core Components

| Component | Responsibility |
|---|---|
| Data Generator Script | Generates daily operational CSV datasets |
| FastAPI Service | Exposes generated datasets through REST APIs |
| File Discovery Endpoint | Dynamically identifies available source files |
| API Ingestion Notebook | Controls ingestion orchestration and monitoring |
| Databricks Volumes | Stores ingested parquet datasets |
| Alerting Framework | Sends ingestion status notifications |
| Monitoring Logic | Tracks ingestion outcomes and anomalies |

## Dynamic File Discovery

The ingestion layer uses a dedicated FastAPI endpoint to dynamically discover newly generated source files before ingestion begins.

Instead of relying on hardcoded processing dates, the ingestion framework queries the API discovery endpoint to identify available datasets for each source entity.

This approach improves:

- Pipeline flexibility
- Operational scalability
- Dynamic partition handling
- Automated discovery workflows
- Reduced manual intervention

The `/files` endpoint returns a source-to-date mapping representing all currently available datasets exposed by the API service.

## File-Level Idempotency

The ingestion framework implements file-level idempotency to prevent duplicate ingestion of previously processed datasets.

Before ingestion begins, the pipeline validates whether data already exists for a given source and partition combination inside Databricks Volumes.

If an existing dataset is detected:

- The ingestion is skipped
- The dataset is marked as already processed
- Monitoring metrics are updated
- Operational alerts reflect the skip behavior

This mechanism ensures safe reruns and recovery-safe ingestion behavior without creating duplicate data.

## Operational Alerting

The ingestion layer generates Slack-based operational alerts to provide visibility into ingestion outcomes and anomalies.

Alert categories include:

- Successful ingestion
- Skipped datasets
- Empty source files
- API failures
- Missing datasets
- Partial ingestion scenarios
- Critical ingestion failures

Alert severity dynamically adjusts based on ingestion outcomes and processing health.

## Failure Handling Strategy

The ingestion layer includes defensive failure handling mechanisms to improve operational reliability.

Implemented controls include:

- HTTP response validation
- Empty dataset detection
- CSV parsing validation
- Exception capture and monitoring
- Missing source detection
- Partial ingestion tracking
- Controlled downstream stopping

The ingestion framework prevents downstream pipeline execution when ingestion health conditions are not satisfied.

## Ingestion Processing States

| Status | Description |
|---|---|
| SUCCESS | Dataset successfully ingested |
| SKIPPED | Dataset already processed |
| EMPTY | Source file contains no records |
| FAILED | Ingestion failure occurred |
| MISSING | Expected dataset not discovered |
| PARTIAL_SUCCESS | Mixed ingestion outcomes detected |
| NO_NEW_DATA | No new datasets available for processing |