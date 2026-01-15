# Day 1 – S3 Data Lake Foundation (Raw Zone)

## Objective
The goal of Day 1 was to establish a **production-grade S3 data lake foundation** for CMS Open Payments data.  
This includes secure storage, proper folder conventions, controlled access via IAM, and a repeatable ingestion path from compute to S3.

This step intentionally focuses on **raw data ingestion only** — no transformations, schema inference, or analytics yet.

---

## S3 Bucket
- **Bucket name:** `open-payments-1759a`
- **Region:** us-west-1
- **Purpose:** Central raw data lake for Open Payments ingestion

---

## Prefix / Folder Structure (Raw Zone)

```text
s3://open-payments-1759a/
└── raw/
    ├── year=2023/
    │   └── *.csv
    ├── year=2024/
    │   └── *.csv
    └── openpayments_companies_totals_by_year.json
