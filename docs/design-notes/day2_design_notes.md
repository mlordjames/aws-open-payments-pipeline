Day 2 – Glue + Athena (Design Notes)
=================================

Objective
---------
Make raw Open Payments data stored in S3 queryable using AWS Glue Data Catalog and Amazon Athena.
This day focuses strictly on schema discovery, SQL access, and validation.

What Exists Before Day 2
------------------------
- S3 bucket: open-payments-1759a (us-west-1)
- Raw data layout:
  s3://open-payments-1759a/raw/year=2023/
  s3://open-payments-1759a/raw/year=2024/
- EC2 ingestion runner already validated

Services Introduced Today
-------------------------
- AWS Glue (Crawler + Data Catalog)
- Amazon Athena
- IAM (Glue service role)
- S3 (Athena query results)

Step-by-Step (Console First)
-----------------------------
1. Create Glue database: open_payments_raw
2. Create IAM role: glue-open-payments-crawler-role
3. Create Glue crawler:
   - Name: open-payments-raw-crawler
   - S3 target: s3://open-payments-1759a/raw/
   - Database: open_payments_raw
   - Table prefix: raw_
   - Schedule: On-demand
4. Run crawler and verify tables
5. Configure Athena results location
6. Run validation SQL queries

Expected Outcome
----------------
- Glue Data Catalog tables for raw data
- Athena SQL access for validation
- No transformations yet

Next
----
Day 3: Glue ETL → curated Parquet datasets
