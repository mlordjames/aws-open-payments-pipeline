#!/usr/bin/env python3
"""
openpayments_companies_totals_by_year.py

1) Fetch ALL company IDs via the datastore query (paginated).
2) For each company ID, call:
   https://openpaymentsdata.cms.gov/api/1/entities/companies/{id}
   (NO year param needed)
3) Extract summaryByAvailableYear[].generalTransactions per year:
   total_2018 .. total_2024
4) Save JSON:
   openpayments_companies_totals_by_year.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests


# ---------------------------
# CONFIG
# ---------------------------
FIRST_API_URL = (
    "https://openpaymentsdata.cms.gov/api/1/datastore/query/"
    "1cf0c6c0-c377-466e-b78f-037b442559f8/0"
)
COMPANY_API_BASE = "https://openpaymentsdata.cms.gov/api/1/entities/companies"


# ---------------------------
# LOGGING
# ---------------------------
def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


# ---------------------------
# PROGRESS BAR (no deps)
# ---------------------------
def render_progress(done: int, total: int, width: int = 30) -> str:
    if total <= 0:
        return ""
    ratio = min(max(done / total, 0.0), 1.0)
    filled = int(ratio * width)
    bar = "█" * filled + "░" * (width - filled)
    pct = int(ratio * 100)
    return f"[{bar}] {pct:3d}% ({done}/{total})"


def print_progress(done: int, total: int) -> None:
    msg = render_progress(done, total)
    sys.stdout.write("\r" + msg)
    sys.stdout.flush()
    if done >= total:
        sys.stdout.write("\n")


# ---------------------------
# HTTP SESSION
# ---------------------------
def requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "accept": "application/json, text/plain, */*",
            "user-agent": "openpayments-companies-totals-by-year/1.0",
        }
    )
    return s


# ---------------------------
# STEP 1: FETCH ALL COMPANY IDS
# ---------------------------
def fetch_all_company_ids(
    session: requests.Session,
    limit: int = 100,
    country: str = "UNITED STATES",
    timeout: int = 60,
) -> List[str]:
    base_params = {
        "keys": "true",
        "limit": limit,
        "conditions[0][property]": "amgpo_making_payment_country",
        "conditions[0][value]": country,
        "conditions[0][operator]": "=",
        "sorts[0][property]": "amgpo_making_payment_name",
        "sorts[0][order]": "asc",
    }

    all_ids: List[str] = []
    offset = 0

    while True:
        params = dict(base_params)
        params["offset"] = offset

        r = session.get(FIRST_API_URL, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()

        results = data.get("results", []) or []
        if not results:
            break

        batch_ids = [
            str(item["amgpo_making_payment_id"])
            for item in results
            if item.get("amgpo_making_payment_id") is not None
        ]
        all_ids.extend(batch_ids)

        logging.info("Fetched %d IDs (offset %d -> %d)", len(batch_ids), offset, offset + len(results) - 1)
        offset += limit

    # De-dupe while preserving order
    seen = set()
    deduped: List[str] = []
    for cid in all_ids:
        if cid not in seen:
            seen.add(cid)
            deduped.append(cid)

    logging.info("Total IDs found: %d", len(deduped))
    return deduped


# ---------------------------
# STEP 2: EXTRACT TOTALS BY YEAR
# ---------------------------
def extract_totals_by_year(payload: Any, years: List[int]) -> Dict[str, Optional[int]]:
    """
    From payload["summaryByAvailableYear"], map programYear -> generalTransactions
    into columns: total_YYYY

    Missing years become 0 (or you can change to None if you prefer).
    """
    out: Dict[str, Optional[int]] = {f"total_{y}": 0 for y in years}

    if not isinstance(payload, dict):
        return out

    items = payload.get("summaryByAvailableYear")
    if not isinstance(items, list):
        return out

    for row in items:
        if not isinstance(row, dict):
            continue
        py = row.get("programYear")
        gt = row.get("generalTransactions")

        # Only map numeric years we care about
        if py is None:
            continue
        try:
            py_int = int(str(py))
        except Exception:
            continue

        if py_int in years:
            # generalTransactions is expected numeric; fallback safe cast
            try:
                out[f"total_{py_int}"] = int(gt) if gt is not None else 0
            except Exception:
                out[f"total_{py_int}"] = 0

    return out


def fetch_company_totals_by_year(
    session: requests.Session,
    company_id: str,
    years: List[int],
    timeout: int = 60,
    max_retries: int = 3,
    backoff_base: float = 1.6,
) -> Tuple[str, Dict[str, Optional[int]], Optional[str]]:
    """
    Returns (company_id, totals_dict, error_message)
    """
    url = f"{COMPANY_API_BASE}/{company_id}"

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            payload = r.json()
            totals = extract_totals_by_year(payload, years)
            return company_id, totals, None
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            if attempt >= max_retries:
                return company_id, {f"total_{y}": 0 for y in years}, msg
            time.sleep(backoff_base ** attempt)

    return company_id, {f"total_{y}": 0 for y in years}, "Unknown error"


# ---------------------------
# MAIN
# ---------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Open Payments: company totals by year (generalTransactions).")
    parser.add_argument("--workers", type=int, default=10, help="Thread workers (default: 10)")
    parser.add_argument("--limit", type=int, default=100, help="Pagination limit for ID query (default: 100)")
    parser.add_argument("--country", default="UNITED STATES", help="Country filter (default: UNITED STATES)")
    parser.add_argument("--min-year", type=int, default=2018, help="Min year column (default: 2018)")
    parser.add_argument("--max-year", type=int, default=2024, help="Max year column (default: 2024)")
    parser.add_argument("--out", default="openpayments_companies_totals_by_year.json", help="Output JSON path")
    parser.add_argument("--verbose", action="store_true", help="Verbose logs")
    args = parser.parse_args()

    setup_logging(args.verbose)

    years = list(range(args.min_year, args.max_year + 1))
    session = requests_session()

    # 1) Fetch IDs
    company_ids = fetch_all_company_ids(
        session=session,
        limit=args.limit,
        country=args.country,
    )
    if not company_ids:
        logging.warning("No company IDs returned. Exiting.")
        return

    # 2) Fetch company totals concurrently
    total = len(company_ids)
    logging.info("Fetching totals for %d companies with %d workers...", total, args.workers)

    rows: List[Dict[str, Any]] = []
    done = 0

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(fetch_company_totals_by_year, session, cid, years): cid
            for cid in company_ids
        }

        for fut in as_completed(futures):
            cid, totals, err = fut.result()
            row = {"company_id": cid, **totals, "error": err}
            rows.append(row)

            done += 1
            print_progress(done, total)

    df = pd.DataFrame(rows).sort_values("company_id").reset_index(drop=True)

    # Save JSON
    df.to_json(args.out, orient="records", indent=2)
    ok = df["error"].isna().sum()
    fail = df["error"].notna().sum()

    logging.info("Saved %d rows to %s", len(df), args.out)
    logging.info("Success: %d | Failed: %d", ok, fail)


if __name__ == "__main__":
    main()
