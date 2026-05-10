# Bronze Layer Documentation

## Overview

The Bronze layer is responsible for raw data standardization, ingestion tracking, schema enforcement, batch-level processing, and operational observability.

The layer processes parquet datasets ingested into Databricks Volumes through the API ingestion framework and converts them into structured Bronze Delta tables for downstream processing.

Each source entity is processed independently through dedicated Bronze notebooks to improve modularity, orchestration control, recovery handling, and debugging workflows.

## Bronze Processing Workflow

```text
Databricks Volumes (Raw Parquet)
        ↓
Bronze Orchestration Notebook
        ↓
Partition Detection
        ↓
Entity-Specific Bronze Notebook Execution
        ↓
Schema Standardization
        ↓
Metadata Enrichment
        ↓
Audit Logging
        ↓
Bronze Delta Tables
        ↓
Monitoring & Alerts
```

## Bronze Layer Responsibilities

| Responsibility | Description |
|---|---|
| Raw Data Ingestion | Reads parquet datasets from Databricks Volumes |
| Schema Standardization | Aligns incoming source structures with Bronze schemas |
| Batch Tracking | Assigns and tracks batch-level processing metadata |
| Metadata Enrichment | Adds ingestion timestamps and operational metadata |
| Audit Logging | Tracks execution stages and operational events |
| Partition Processing | Processes newly detected partitions independently |
| Monitoring Integration | Updates operational monitoring tables |
| Orchestration Support | Supports downstream dependency validation |

## Bronze Orchestration

The Bronze orchestration framework dynamically detects newly arrived partitions and controls notebook execution across all source entities.

The orchestration process performs:

- Partition discovery
- Source grouping
- Table-level notebook execution
- Monitoring updates
- Batch tracking
- Operational alerting

The orchestration layer ensures that only newly arrived datasets are processed while maintaining controlled downstream dependencies.

## Batch Processing Strategy

The Bronze layer uses partition-aware batch processing to improve scalability, execution isolation, and recovery handling.

Each partition is processed independently using generated batch identifiers. This design enables:

- Controlled reruns
- Batch-level observability
- Failure isolation
- Selective recovery workflows
- Downstream dependency enforcement

Batch metadata is propagated downstream into Silver and Gold layers for traceability and operational debugging.

## Metadata Enrichment

During Bronze ingestion, operational metadata columns are appended to all datasets.

Typical metadata includes:

| Metadata Column | Purpose |
|---|---|
| _batch_id | Tracks processing batch execution |
| _ingestion_ts | Captures ingestion timestamp |
| _source_file | Tracks originating source dataset |
| _ingest_date | Tracks operational processing date |

These metadata fields improve lineage tracking, debugging, monitoring, and downstream orchestration control.

## Monitoring & Audit Logging

The Bronze layer integrates operational monitoring and audit logging to improve execution visibility.

Monitoring capabilities include:

- Batch execution tracking
- Partition processing visibility
- Row-level ingestion metrics
- Operational event logging
- File arrival alerts
- Orchestration tracking
- Downstream dependency validation

Audit logs capture important execution stages and operational events throughout processing.

## Failure Handling

The Bronze layer includes recovery-safe execution controls to minimize downstream data quality risks.

Implemented controls include:

- Schema validation
- Partition-level failure isolation
- Controlled orchestration execution
- Audit-based monitoring
- Exception capture and logging
- Silent failure tracking
- Downstream blocking for failed batches

These mechanisms help ensure that downstream layers process only operationally safe Bronze datasets.