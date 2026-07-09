# Cybersecurity Log Analytics Platform - Abstract

This batch-processing platform pre-processes and aggregates cybersecurity events for an anomaly detection model that detects unusual computer behavior in a fictional enterprise network. It is demonstrated on a 14-day subset of the Los Alamos National Laboratory (LANL) dataset, covering authentication, process, network-flow, and DNS events.

The platform follows a lakehouse architecture built from microservices. A Data Producer Service (Python) replays the LANL subset as daily raw batches into the landing bucket of a MinIO object store. A Data Processing Service (Apache Spark) refines these batches through three Medallion layers, each a Delta Lake table: schema-validated ingestion into Bronze, cleaning and transformation into Silver, and aggregation over a configurable 7-day rolling window into Gold. A Data Delivery Service (Python) encrypts each Gold partition using Parquet modular encryption and writes it to a delivered bucket, from which a Mock Anomaly Detection Service consumes the features and simulates retraining. An Airflow Orchestration Service with its own PostgreSQL metadata database schedules all jobs, and every stage records lineage, schema versions, checksums, and delivery manifests in a PostgreSQL Governance Store. All components run as Docker containers in separate networks; GitHub versions the source code and GitHub Actions provides the test and build pipeline.

References:

- A. D. Kent, “Comprehensive, Multi-Source Cyber-Security Events,”
Los Alamos National Laboratory, http://dx.doi.org/10.17021/1179829, 2015.