# ADR 004: Analytical Processing (Gold Layer) and Data Discovery

**Status:** Accepted  
**Date:** May 25, 2026  
**Authors:** Data Architecture Team

## Context & Problem Statement
Business logic and downstream aggregations (the Gold/Spec layer) must be accessible to analysts, but processing this logic via AWS Glue requires specialized PySpark skills. Furthermore, we must maintain a unified data dictionary and unbroken data lineage from the Silver layer to the final Spec layer so business users trust the metrics.

## Decision
We will decouple business transformations from Spark ETL by utilizing **dbt**, **Amazon Athena**, and **Amazon DataZone**.

1. **Serverless Querying:** **Amazon Athena** will serve as the primary interactive query engine for the Silver (`sot`) and Spec (`spec`) buckets.
2. **Business Transformations:** Analysts will use **dbt (data build tool)** to define Gold layer aggregations using standard SQL. dbt will execute these models directly against Amazon Athena, writing the results to `s3://spec-{account_id}`.
3. **End-to-End Lineage:** We will utilize the OpenLineage dbt integration. When dbt executes a model, it emits lineage metadata to **Amazon DataZone**, completing the "glass pipeline" (SFTP -> Glue -> Athena -> dbt -> Dashboard).
4. **Automated Discovery:** **AWS Glue Crawlers** will asynchronously scan both the Silver and Spec buckets to detect schema changes and populate the central DataZone Business Data Catalog.

## Alternatives Considered
* **AWS Glue for Gold Layer:** Rejected. PySpark introduces a high barrier to entry for BI analysts. SQL via dbt is universally understood and natively version-controlled.
* **Amazon Redshift:** Rejected. Given the decoupled storage and unpredictable query volume, Redshift introduces unnecessary provisioned compute costs. Serverless Athena is highly optimized for our compacted Iceberg/Parquet architecture.

## Consequences
* **Positive:** Lowers the barrier to entry; analysts can build data pipelines using pure SQL.
* **Positive:** Maintains strict data lineage and governance within DataZone, ensuring business users can audit exactly how a metric was calculated.
* **Positive:** AWS Glue Crawlers automate the documentation of schema drift without human intervention.