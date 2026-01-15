# Day 2 – Glue & Athena (Schema Discovery and Validation)

## Objective
The objective of Day 2 was to make the raw CMS Open Payments data stored in Amazon S3 queryable via SQL,
using AWS Glue Data Catalog and Amazon Athena.

This stage focuses on schema discovery, exploration, and validation — not transformation.

---

## What Was Completed

### 1. Glue Data Catalog Setup
- Created Glue database:
  - Database name: open_payments_raw
- Configured Glue as the central metadata store for raw datasets.

### 2. Glue Crawler
- Created Glue crawler:
  - Name: open-payments-raw-crawler
  - S3 target: s3://open-payments-1759a/raw/
  - Table prefix: raw_
  - Schedule: On-demand
- Ran the crawler successfully.
- Generated tables:
  - raw_year_2023
  - raw_year_2024

### 3. Athena Configuration
- Configured Athena query results location:
  - s3://open-payments-1759a/athena-results/
- Verified Athena could read from Glue Data Catalog.

### 4. Initial Validation Queries
Executed:
```sql
SELECT COUNT(*) FROM raw_year_2023;
SELECT COUNT(*) FROM raw_year_2024;

SELECT * FROM raw_year_2023 LIMIT 10;
```

---

## Issues Encountered

### Issue 1: All Columns Inferred as STRING
- Cause: Mixed data formats and nulls in CSVs.
- Workaround:
```sql
SELECT
  SUM(TRY_CAST(REPLACE(total_amount_of_payment_usdollars, ',', '') AS DOUBLE))
FROM raw_year_2023;
```
- Resolution planned: enforce schema during Glue ETL (Day 3).

---

### Issue 2: Unexpected Extra Columns (col91–col518)
- Cause: CSV parsing inconsistencies (embedded commas, multiline fields, or schema drift).
- Validation:
```sql
SHOW COLUMNS IN raw_year_2023;
```
- Resolution planned:
  - Use manual Athena tables with OpenCSVSerDe (short-term), or
  - Fix via Glue ETL and Parquet output (preferred).

---

## Key Learnings
- Crawlers are conservative with CSV inference.
- Athena is ideal for validation, not schema enforcement.
- Raw zones tolerate messiness; curated zones enforce structure.

---

## Status at End of Day 2
- Raw data queryable
- Issues identified and documented
- Ready for Day 3 transformations

---

## Next: Day 3
- Glue ETL (PySpark)
- Clean, deduplicate, type-cast
- Write Parquet to curated zone
