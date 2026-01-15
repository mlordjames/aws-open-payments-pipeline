#!/usr/bin/env python3
"""
Audit raw Open Payments CSVs in S3 for:
- Header column match against expected schema
- Missing/extra columns
- Bad-line signals (inconsistent column counts) using a sample of first N lines
- CSV parsing errors (quote/newline issues) during sampling

Outputs a CSV report you can commit to Git.
"""

import argparse
import csv
import io
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple, Optional

import boto3
from botocore.exceptions import ClientError


EXPECTED_COLUMNS = [
    'Change_Type', 'Covered_Recipient_Type', 'Teaching_Hospital_CCN',
    'Teaching_Hospital_ID', 'Teaching_Hospital_Name',
    'Covered_Recipient_Profile_ID', 'Covered_Recipient_NPI',
    'Covered_Recipient_First_Name', 'Covered_Recipient_Middle_Name',
    'Covered_Recipient_Last_Name', 'Covered_Recipient_Name_Suffix',
    'Recipient_Primary_Business_Street_Address_Line1',
    'Recipient_Primary_Business_Street_Address_Line2', 'Recipient_City',
    'Recipient_State', 'Recipient_Zip_Code', 'Recipient_Country',
    'Recipient_Province', 'Recipient_Postal_Code',
    'Covered_Recipient_Primary_Type_1', 'Covered_Recipient_Primary_Type_2',
    'Covered_Recipient_Primary_Type_3', 'Covered_Recipient_Primary_Type_4',
    'Covered_Recipient_Primary_Type_5', 'Covered_Recipient_Primary_Type_6',
    'Covered_Recipient_Specialty_1', 'Covered_Recipient_Specialty_2',
    'Covered_Recipient_Specialty_3', 'Covered_Recipient_Specialty_4',
    'Covered_Recipient_Specialty_5', 'Covered_Recipient_Specialty_6',
    'Covered_Recipient_License_State_code1',
    'Covered_Recipient_License_State_code2',
    'Covered_Recipient_License_State_code3',
    'Covered_Recipient_License_State_code4',
    'Covered_Recipient_License_State_code5',
    'Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country',
    'Total_Amount_of_Payment_USDollars', 'Date_of_Payment',
    'Number_of_Payments_Included_in_Total_Amount',
    'Form_of_Payment_or_Transfer_of_Value',
    'Nature_of_Payment_or_Transfer_of_Value', 'City_of_Travel',
    'State_of_Travel', 'Country_of_Travel', 'Physician_Ownership_Indicator',
    'Third_Party_Payment_Recipient_Indicator',
    'Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value',
    'Charity_Indicator', 'Third_Party_Equals_Covered_Recipient_Indicator',
    'Contextual_Information', 'Delay_in_Publication_Indicator', 'Record_ID',
    'Dispute_Status_for_Publication', 'Related_Product_Indicator',
    'Covered_or_Noncovered_Indicator_1',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',
    'Product_Category_or_Therapeutic_Area_1',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',
    'Associated_Drug_or_Biological_NDC_1',
    'Associated_Device_or_Medical_Supply_PDI_1',
    'Covered_or_Noncovered_Indicator_2',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2',
    'Product_Category_or_Therapeutic_Area_2',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2',
    'Associated_Drug_or_Biological_NDC_2',
    'Associated_Device_or_Medical_Supply_PDI_2',
    'Covered_or_Noncovered_Indicator_3',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3',
    'Product_Category_or_Therapeutic_Area_3',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3',
    'Associated_Drug_or_Biological_NDC_3',
    'Associated_Device_or_Medical_Supply_PDI_3',
    'Covered_or_Noncovered_Indicator_4',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4',
    'Product_Category_or_Therapeutic_Area_4',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4',
    'Associated_Drug_or_Biological_NDC_4',
    'Associated_Device_or_Medical_Supply_PDI_4',
    'Covered_or_Noncovered_Indicator_5',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5',
    'Product_Category_or_Therapeutic_Area_5',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5',
    'Associated_Drug_or_Biological_NDC_5',
    'Associated_Device_or_Medical_Supply_PDI_5', 'Program_Year',
    'Payment_Publication_Date'
]


@dataclass
class AuditRow:
    year: str
    s3_key: str
    size_bytes: int
    header_col_count: int
    expected_col_count: int
    header_match: str  # YES/NO
    missing_columns: str
    extra_columns: str
    # sampling / bad-lines signals
    sample_lines_checked: int
    sample_bad_line_count: int
    sample_parse_error: str  # empty or error string


def list_s3_objects(s3, bucket: str, prefix: str) -> List[Dict]:
    paginator = s3.get_paginator("list_objects_v2")
    out = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # skip "folders"
            if obj["Key"].endswith("/"):
                continue
            out.append(obj)
    return out


def read_s3_head_text(s3, bucket: str, key: str, max_bytes: int) -> str:
    """
    Read up to max_bytes from the start of an S3 object and return as text.
    """
    rng = f"bytes=0-{max_bytes-1}"
    resp = s3.get_object(Bucket=bucket, Key=key, Range=rng)
    data = resp["Body"].read()
    # attempt utf-8, then fall back
    try:
        return data.decode("utf-8", errors="replace")
    except Exception:
        return data.decode(errors="replace")


def normalize_header(cols: List[str]) -> List[str]:
    # strip BOM and whitespace
    normalized = []
    for i, c in enumerate(cols):
        c2 = c.strip()
        if i == 0:
            c2 = c2.lstrip("\ufeff")
        normalized.append(c2)
    return normalized


def audit_one_file(
    s3,
    bucket: str,
    key: str,
    year: str,
    expected: List[str],
    head_bytes: int,
    sample_rows: int,
) -> AuditRow:
    # Read head chunk (header + sample rows)
    text = read_s3_head_text(s3, bucket, key, head_bytes)

    # Use csv module for header + sampling
    expected_set = set(expected)
    header_cols: List[str] = []
    bad_line_count = 0
    parse_error = ""
    rows_checked = 0

    try:
        reader = csv.reader(io.StringIO(text))
        header_cols = next(reader)
        header_cols = normalize_header(header_cols)

        expected_len = len(expected)

        # Sample first N data rows to detect "bad lines" signals
        for row in reader:
            rows_checked += 1
            if rows_checked > sample_rows:
                break
            if len(row) != len(header_cols):
                # row-length mismatch => likely quoting/newline/comma issue
                bad_line_count += 1

    except StopIteration:
        parse_error = "EMPTY_OR_NO_HEADER"
    except csv.Error as e:
        parse_error = f"CSV_ERROR: {e}"
    except Exception as e:
        parse_error = f"ERROR: {e}"

    header_set = set(header_cols)
    missing = sorted(list(expected_set - header_set))
    extra = sorted(list(header_set - expected_set))

    match = "YES" if (len(missing) == 0 and len(extra) == 0) else "NO"

    # get object size (best-effort)
    size_bytes = 0
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        size_bytes = int(head.get("ContentLength", 0))
    except Exception:
        size_bytes = 0

    return AuditRow(
        year=year,
        s3_key=key,
        size_bytes=size_bytes,
        header_col_count=len(header_cols),
        expected_col_count=len(expected),
        header_match=match,
        missing_columns=";".join(missing),
        extra_columns=";".join(extra),
        sample_lines_checked=rows_checked,
        sample_bad_line_count=bad_line_count,
        sample_parse_error=parse_error,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Audit raw Open Payments CSV schema in S3.")
    p.add_argument("--bucket", required=True, help="S3 bucket name (e.g., open-payments-1759a)")
    p.add_argument("--years", nargs="+", required=True, help="Years to audit (e.g., 2023 2024)")
    p.add_argument(
        "--prefix-template",
        default="raw/year={year}/",
        help="S3 prefix template. Use {year}. Examples: raw/year={year}/ OR raw/{year}/",
    )
    p.add_argument(
        "--head-bytes",
        type=int,
        default=10 * 1024 * 1024,
        help="Bytes to read from start of each file (default 10MB). Increase if headers span large chunk.",
    )
    p.add_argument(
        "--sample-rows",
        type=int,
        default=2000,
        help="How many data rows to sample for bad-line signals (default 2000).",
    )
    p.add_argument(
        "--output",
        default="raw_schema_audit_report.csv",
        help="Local output CSV filename.",
    )
    args = p.parse_args()

    s3 = boto3.client("s3")

    rows: List[AuditRow] = []
    for year in args.years:
        prefix = args.prefix_template.format(year=year)
        print(f"[INFO] Listing s3://{args.bucket}/{prefix}")
        objects = list_s3_objects(s3, args.bucket, prefix)
        print(f"[INFO] Found {len(objects)} objects for year={year}")

        for obj in objects:
            key = obj["Key"]
            # only CSVs
            if not key.lower().endswith(".csv"):
                continue
            print(f"[AUDIT] {key}")
            r = audit_one_file(
                s3=s3,
                bucket=args.bucket,
                key=key,
                year=year,
                expected=EXPECTED_COLUMNS,
                head_bytes=args.head_bytes,
                sample_rows=args.sample_rows,
            )
            rows.append(r)

    # Write report
    fieldnames = list(asdict(rows[0]).keys()) if rows else list(AuditRow(
        year="", s3_key="", size_bytes=0, header_col_count=0, expected_col_count=0,
        header_match="", missing_columns="", extra_columns="",
        sample_lines_checked=0, sample_bad_line_count=0, sample_parse_error=""
    ).__dict__.keys())

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(asdict(r))

    print(f"[DONE] Wrote report: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
