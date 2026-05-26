# ADR 002: Lazy Compaction and Resilient Error Handling for Silver Layer

**Status:** Accepted  
**Date:** May 25, 2026  
**Authors:** Data Architecture Team

## Context & Problem Statement
The micro-batching ingestion pipeline (ADR 001) continuously fragments the S3 Silver layer (`sot`) with small Parquet files, degrading Amazon Athena query performance. Eager, daily compaction of the entire data lake is computationally wasteful. We need a cost-effective, asynchronous mechanism to compact targeted partitions without allowing a single corrupted file to crash the maintenance pipeline.

## Decision
We will implement an automated **Lazy Compaction** architecture driven by SQS, CloudWatch, and Step Functions.

1. **Accumulation:** S3 `PutObject` events in the Silver layer accumulate in an **Amazon SQS Queue** (`sot-coming-files`).
2. **Zero-Cost Trigger:** An **Amazon CloudWatch Alarm** monitors the queue's age/size metric and triggers an **AWS Step Functions** state machine (`sf-small-file-fixer`) strictly when thresholds are met.
3. **Queue Draining:** An AWS Lambda task (`sot-comming`) drains the SQS queue, extracts distinct S3 prefixes, and returns a deduplicated array.
4. **Fan-Out Execution:** Step Functions executes an inline **Map State** to process the array concurrently, spawning tightly scoped **AWS Glue jobs** (`etl-small-file-fixer`) to coalesce files and overwrite partitions.
5. **Isolated Error Handling:** Step Functions `Catch` blocks isolate failures. Failed partitions route to a DLQ (`sot-comming-files.dlq`), while healthy partitions finish successfully. Any DLQ events trigger an **Amazon SES** alert.

## Consequences
* **Positive:** Zero idle compute costs. The CloudWatch Alarm ensures orchestration sleeps until there is sufficient work to do.
* **Positive:** Drastically lowers AWS Glue compute costs by avoiding unnecessary lake-wide scans.
* **Positive:** High operational resilience; one bad partition does not halt the entire compaction process.