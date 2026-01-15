# AWS Open Payments Analytics Pipeline (Production-Grade Foundations)

A production-minded AWS data platform built on **real CMS Open Payments data**.
This project creates a governed S3 data lake, enables serverless SQL analytics via Athena,
and evolves into a curated Parquet layer with Glue jobs and orchestration with Airflow.

## Problem
Healthcare, insurance, and compliance teams need to analyze financial relationships between providers and vendors at scale.
CMS Open Payments provides public, multi-million row datasets, but they are massive CSVs, fragmented, and not query-friendly.

## Goals (Days 1–4)
- Ingest real CMS Open Payments CSVs to AWS S3 with auditability
- Create a **raw → cleaned → curated** data lake layout
- Discover schema with AWS Glue + query with Athena
- Track ingestion metadata in DynamoDB
- Transform into curated Parquet using Glue PySpark
- Orchestrate runs with Airflow (local), discuss MWAA and warehouse options

## Data Source (REAL)
- CMS Open Payments: https://openpaymentsdata.cms.gov/

Datasets:
- General Payments
- Research Payments
- Ownership / Investment Interests

## Architecture (Day 1 baseline)
- S3 data lake (versioned raw zone, curated zones)
- (Next) Glue Crawler for catalog
- (Next) Athena for SQL analytics
- (Next) DynamoDB for ingestion metadata

## Repo Structure
- `infra/` — Infrastructure as Code (CloudFormation now; Terraform later)
- `ingestion/` — Scripts/utilities to download + upload datasets
- `glue_jobs/` — Glue ETL (PySpark) jobs for cleaned/curated tables
- `airflow/` — Local DAGs for orchestration
- `sql/` — Athena and (optional) Redshift queries
- `docs/architecture/` — Diagrams & notes

## S3 Data Lake Layout
```text
s3://<bucket>/
  raw/
    year=2022/
    year=2023/
  cleaned/
  curated/

```md
## Day 1 – S3 Data Lake Foundation

### Objective
Establish a secure, auditable S3 raw data lake for real CMS Open Payments datasets.

### What Was Done
- Created an S3 bucket to serve as the raw data lake
- Designed a year-partitioned raw folder structure
- Provisioned an EC2 runner for long-running ingestion scripts
- Configured an IAM role with write-only access to the raw zone
- Uploaded real Open Payments data into S3
- Verified access and integrity using AWS CLI

### Key Commands Used
```bash
aws sts get-caller-identity
aws s3 ls s3://open-payments-1759a/raw/
aws s3 cp local_file.csv s3://open-payments-1759a/raw/year=2023/