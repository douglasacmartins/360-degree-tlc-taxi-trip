# ADR 001: Secure Ingestion and Event-Driven Micro-Batching

**Status:** Accepted  
**Date:** May 25, 2026  
**Authors:** Data Architecture Team

## Context & Problem Statement
External actors and systems need a secure, reliable way to upload NYC Taxi trip data. Once landed in the Bronze layer, the data must be normalized and written to the Silver layer (Parquet) with captured metadata. Relying on direct S3 API uploads creates security friction and training overhead for business users. Furthermore, triggering an ETL job for every single file is cost-prohibitive and breaches concurrency limits.

## Decision
We will implement an **SFTP-backed Event-Driven Micro-Batching** architecture.

1. **Secure Ingestion:** We will use **AWS Transfer Family** to provision a fully managed SFTP endpoint mapped directly to our Bronze S3 bucket (`s3://sor-{account_id}`).
2. **Event Capture:** SFTP uploads trigger S3 `ObjectCreated` events, which are routed to an **Amazon SQS Queue** (`sor-landing-files-events`).
3. **Batching:** **Amazon EventBridge Pipes** will poll the queue, batching events by size (256 KB) or time (5 minutes).
4. **Orchestration & ETL:** EventBridge Pipes triggers an **AWS Step Functions** state machine, which orchestrates an **AWS Glue Job** (`etl-silver`).
5. **Governance:** The Glue job normalizes the data, emits lineage to **Amazon DataZone** via OpenLineage, and writes partitioned Parquet to the Silver bucket (`s3://sot-{account_id}`). Failed ingestions route to a Dead-Letter Queue (DLQ) and trigger an **Amazon SES** alert.

## Alternatives Considered
* **Direct S3 Console/CLI Uploads:** Rejected. Requires managing complex IAM user policies for external actors and lacks the universal compatibility of SFTP.
* **Direct Lambda-to-Glue Trigger:** Rejected. Fails to handle traffic spikes safely, leading to API throttling and race conditions.

## Consequences
* **Positive:** SFTP provides a frictionless, heavily isolated entry point for external data providers.
* **Positive:** EventBridge Pipes and SQS act as a shock absorber, protecting AWS Glue from sudden traffic spikes.
* **Negative (Mitigated):** Writing in continuous micro-batches creates a "small file problem" in the Silver layer. *Mitigation:* This is resolved asynchronously by a separate "Lazy Compaction" process (See ADR 002).