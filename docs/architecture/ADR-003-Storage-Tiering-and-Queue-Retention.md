# ADR 003: Storage Tiering, Lifecycle Management, and Queue Retention

**Status:** Accepted  
**Date:** May 25, 2026  
**Authors:** Data Architecture Team

## Context & Problem Statement
S3 storage and SQS state management will grow linearly if retention is not strictly managed. Additionally, analytical access patterns on the Silver layer are highly unpredictable. We must define strict lifecycle policies to ensure FinOps compliance without penalizing data discovery.

## Decision

### 1. Amazon S3 Storage Classes
* **Bronze Layer (`sor`):** * **Class:** S3 Standard.
  * **Lifecycle Policy:** Expire (Permanently Delete) after **30 days**. (Raw data is ephemeral once lineage is captured).
* **Silver Layer (`sot`):**
  * **Class:** **S3 Intelligent-Tiering**.
  * **Lifecycle Policy:** N/A (Managed natively).
  * *Architectural Dependency:* Intelligent-Tiering ignores objects under 128KB and charges a monitoring fee. **ADR 002 (Lazy Compaction)** is mandatory to make this financially viable, as it merges tiny files into large Parquet blocks, unlocking zero-cost retrieval for analytical queries.

### 2. Amazon SQS Queue Configuration
* **Queue Type:** SQS Standard (Provides infinite throughput; downstream logic handles deduplication natively).
* **Message Retention Period:** 14 Days (Maximum safety net for extended compute outages).
* **Dead-Letter Queue (DLQ):** Mandatory for all queues, integrated with Amazon SES for automated alerting.

## Consequences
* **Positive:** Intelligent-Tiering provides optimal FinOps scaling with zero manual lifecycle guessing and zero Athena retrieval penalties.
* **Positive:** Aggressive Bronze expiration puts a hard cap on raw storage costs.