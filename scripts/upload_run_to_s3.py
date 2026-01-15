import argparse
import hashlib
from pathlib import Path
from typing import Iterable, Tuple

import boto3
from botocore.exceptions import ClientError


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def s3_key_for_year_file(local_file: Path, year_dir: Path) -> str:
    rel = local_file.relative_to(year_dir).as_posix()
    year = year_dir.name
    return f"raw/year={year}/{rel}"


def upload_file(
    s3_client,
    bucket: str,
    local_path: Path,
    key: str,
) -> None:
    extra_args = {}

    if local_path.suffix.lower() == ".csv":
        extra_args["ContentType"] = "text/csv"
    elif local_path.suffix.lower() == ".json":
        extra_args["ContentType"] = "application/json"

    s3_client.upload_file(
        Filename=str(local_path),
        Bucket=bucket,
        Key=key,
        ExtraArgs=extra_args if extra_args else None,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload Open Payments run outputs to S3 and delete local files.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--base-dir", default=".")
    parser.add_argument("--years", nargs="+", default=["2023", "2024"])
    parser.add_argument("--totals-json", default="openpayments_companies_totals_by_year.json")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    bucket = args.bucket
    base_dir = Path(args.base_dir).resolve()
    totals_path = base_dir / args.totals_json

    s3 = boto3.client("s3")

    print("Skipping HeadBucket check; proceeding with uploads using PutObject permissions.")

    uploads: list[Tuple[Path, str]] = []

    # Year folders
    for y in args.years:
        year_dir = base_dir / y
        if not year_dir.is_dir():
            raise SystemExit(f"Year folder not found: {year_dir}")

        for f in iter_files(year_dir):
            uploads.append((f, s3_key_for_year_file(f, year_dir)))

    # Totals JSON
    if not totals_path.exists():
        raise SystemExit(f"Totals JSON not found: {totals_path}")

    uploads.append((totals_path, f"raw/{totals_path.name}"))

    print(f"Base dir: {base_dir}")
    print(f"Bucket:   {bucket}")
    print(f"Files to move: {len(uploads)}")

    for local_path, key in uploads:
        if args.dry_run:
            print(f"[DRY RUN] {local_path} -> s3://{bucket}/{key}")
            continue

        try:
            upload_file(s3, bucket, local_path, key)
            local_path.unlink()  # 🔥 delete only after successful upload
            print(f"[OK] Uploaded & deleted: {local_path}")

        except ClientError as e:
            print(f"[ERROR] Failed upload, file NOT deleted: {local_path}")
            print(e)
            continue

    print("Upload & cleanup complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
