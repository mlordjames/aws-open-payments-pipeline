Design Notes

raw/ is immutable by convention (no in-place edits).

Year-based partitioning is used to support future Athena/Glue partition pruning.

Summary metadata (openpayments_companies_totals_by_year.json) is stored at the root of raw/ for now.

EC2 Runner (Ingestion Compute)

An EC2 instance is used as a temporary ingestion runner to:

Execute long-running Python scripts (not suitable for Lambda)

Pull source code from a public GitHub repository

Upload extracted results to S3 using an IAM role (no access keys)

Characteristics

OS: Amazon Linux 2023

Instance type: t2.small

Access: SSH

Purpose: Ad-hoc ingestion & experimentation (not permanent infrastructure)

IAM Role & Security
IAM Role

Role name: open-payments-ec2-runner-role

Attached to: EC2 instance via instance profile

Permissions (Principle of Least Privilege)

The role is intentionally write-only to the raw zone.

Allowed actions:

Upload objects to:
s3://open-payments-1759a/raw/*

List objects under raw/ prefix

Read bucket location (required by AWS SDKs)

Explicitly not allowed:

Deleting S3 objects

Reading data outside raw/

Accessing other buckets

No access keys are used; authentication is handled via the EC2 instance role.

Data Movement Strategy

A custom Python uploader script is used to:

Upload local year folders (2023/, 2024/) into raw/year=YYYY/

Upload summary JSON to raw/

Delete local files only after successful upload

Support dry-run mode for safety

This mimics a safe mv (copy → verify → delete) workflow.

Validation & Verification

The following checks were performed to validate Day 1 setup:

IAM / Identity
aws sts get-caller-identity


Confirmed the EC2 instance assumed the correct IAM role.

S3 Write Test
echo "hello" > /tmp/s3_test.txt
aws s3 cp /tmp/s3_test.txt s3://open-payments-1759a/raw/s3_test.txt


Confirmed write access to the raw prefix.

Raw Zone Listing
aws s3 ls s3://open-payments-1759a/raw/
aws s3 ls s3://open-payments-1759a/raw/year=2023/
aws s3 ls s3://open-payments-1759a/raw/year=2024/


Verified that extracted datasets landed in the expected locations.

Status at End of Day 1

✅ Raw data successfully ingested into S3

✅ Secure, role-based access configured

✅ Folder structure aligned with data lake best practices

✅ Ready for schema discovery and SQL access (Day 2)

Next step: AWS Glue Crawler + Athena queries.


---

# 📄 README.md — **Day 1 section to add**

Add this section to your main `README.md`:

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

Outcome

A production-ready raw data lake foundation that supports future schema discovery, analytics, and automated pipelines.


---

## What to do next (quick checklist)
- Review & edit wording to match your voice
- Commit:
  ```bash
  git add docs/day1_s3_setup.md README.md
  git commit -m "docs: Day 1 S3 raw data lake setup"
  git push


Tomorrow, we move cleanly into Day 2: Glue + Athena.