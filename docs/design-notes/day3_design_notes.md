Day 3 – Design Notes

Problem:
Raw CSV data is unreliable for analytics due to lack of schema and parsing ambiguity.

Solution:
Introduce a curated data layer using Glue ETL and Parquet.

Key Principles:
- Raw data is immutable
- Curated data is authoritative
- Schema enforced during transformation
- Optimized storage for analytics

ETL Responsibilities:
- Read raw CSV defensively
- Select trusted columns
- Cast data types
- Normalize values
- Deduplicate
- Write Parquet partitioned by year

Problem:
seems the files downloaded may not be very correct, so lets examin the files and redownload them again.
so build a schema audits to check the files in the s3
