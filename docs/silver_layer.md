# Silver Layer Documentation

## Overview

The Silver layer is responsible for data validation, business rule enforcement, quarantine handling, deduplication, incremental merge processing, and recovery-safe downstream preparation.

Validated Bronze datasets are transformed into curated Silver Delta tables where operational quality checks, business validation rules, and merge strategies are applied before downstream analytical consumption.

The Silver layer was intentionally designed to simulate production-style operational controls with strong emphasis on data quality, monitoring, failure isolation, and recovery handling.

## Silver Processing Workflow

```text
Bronze Delta Tables
        ↓
Silver Orchestration
        ↓
Batch Validation
        ↓
Business Rule Enforcement
        ↓
Quarantine Isolation
        ↓
Validation Metrics Tracking
        ↓
Threshold Validation
        ↓
Deduplication
        ↓
Incremental Merge Processing
        ↓
Silver Delta Tables
        ↓
Monitoring & Alerts
```

## Silver Layer Responsibilities

| Responsibility | Description |
|---|---|
| Business Validation | Applies operational data quality rules |
| Quarantine Handling | Isolates invalid records into quarantine tables |
| Threshold Enforcement | Stops processing for excessive invalid records |
| Deduplication | Removes duplicate records using window logic |
| Merge Processing | Incrementally updates Silver Delta tables |
| Batch Monitoring | Tracks processing metrics and validation outcomes |
| Silent Failure Detection | Identifies partially processed datasets |
| Recovery Support | Enables controlled manual reruns |
| Downstream Protection | Prevents invalid data propagation |

## Validation Framework

The Silver layer applies entity-specific business validation rules before data is allowed into curated Silver tables.

Validation categories implemented in the project include:

- Null validation
- Empty string validation
- Invalid numeric checks
- Negative value validation
- Invalid timestamp handling
- Future date validation
- Schema casting validation
- Business rule enforcement

Validation outcomes are tracked using quarantine reason labels to improve debugging and operational visibility.

## Quarantine Strategy

Invalid records identified during validation are isolated into dedicated quarantine tables instead of being discarded.

Each quarantined record includes:

| Metadata | Purpose |
|---|---|
| quarantine_reason | Captures validation failure reason |
| _bronze_batch_id | Tracks originating Bronze batch |
| _bronze_ingestion_ts | Tracks source ingestion timestamp |
| _silver_quarantined_ts | Tracks quarantine timestamp |

The quarantine framework improves:

- Failure investigation
- Recovery workflows
- Data quality auditing
- Operational debugging
- Validation transparency

## Threshold-Based Failure Handling

The Silver layer enforces configurable invalid record thresholds to prevent large-scale propagation of poor-quality datasets.

If the percentage of invalid records exceeds the configured threshold:

- The batch is marked as FAILED
- Downstream execution is blocked
- Monitoring metrics are updated
- Operational alerts are triggered

This mechanism improves downstream data reliability and operational safety.

## Deduplication Strategy

The Silver layer uses window-function-based deduplication to retain the most recent valid record for each business entity key combination.

Deduplication logic prioritizes:

1. Latest business update timestamp
2. Latest Bronze ingestion timestamp

This strategy ensures deterministic record selection while supporting late-arriving and reprocessed datasets.

## Incremental Merge Processing

Validated and deduplicated datasets are incrementally merged into Silver Delta tables using controlled merge strategies.

Merge operations support:

- INSERT handling for new records
- UPDATE handling for newer records
- LATE record detection
- STALE record detection

The merge framework ensures that Silver tables maintain the latest valid business state while preserving operational consistency.

## Monitoring & Operational Metrics

The Silver layer captures extensive operational metrics throughout processing.

Tracked metrics include:

- Bronze rows read
- Validated rows
- Quarantined rows
- Deduplicated rows
- Merge outcomes
- Validation failures
- Threshold breaches
- Batch execution status
- Silent failure tracking

These metrics improve observability, debugging, and operational governance.

## Recovery & Failure Isolation

The Silver layer was intentionally designed for recovery-safe execution and selective reprocessing.

Recovery capabilities include:

- Table-level reruns
- Batch-level recovery
- Quarantine inspection
- Silent failure debugging
- Controlled downstream blocking
- Monitoring-driven troubleshooting

This design minimizes cascading failures and simplifies operational debugging workflows.