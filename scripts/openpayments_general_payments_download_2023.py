#!/usr/bin/env python3
"""
openpayments_general_payments_download_2023.py

Downloads Open Payments "General Payments" rows for 2023 by company_id,
based on totals file openpayments_companies_totals_by_year.json.

2023 dataset id:
  74707c0a-5cf5-5b1a-a8b8-53588d660e9a

Rules:
- Uses 2023 dataset ID (NO Program_Year filter, NO --years arg)
- Skip total_2023 == 0
- total_2023 <= 5000: sequential paging (until empty page)
- total_2023 > 5000: page-parallel downloads (known number of pages), then merge
- Save to folder: ./2023/
- Optional slicing: --slice "0:10" or --slice "90:-1"

Strict "no results" rule:
- Header-only => FAIL

Logging:
- FILE ONLY (no console output)
- One log file per run:
    <out_root>/logs/openpayments_download_2023_<timestamp>.log
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry


# ====================== 2023 DATASET CONFIG ======================
YEAR = 2023
DATASET_ID_2023 = "74707c0a-5cf5-5b1a-a8b8-53588d660e9a"
BASE_URL_2023 = f"https://openpaymentsdata.cms.gov/api/1/datastore/query/{DATASET_ID_2023}/download"
# ================================================================


# ====================== CONFIG ======================
PAGE_LIMIT = 5000

CONNECT_TIMEOUT = 60
READ_TIMEOUT = 180

# urllib3 Retry (does NOT include 403/429)
MAX_RETRIES = 4
BACKOFF_FACTOR = 0.8

# Manual retry for 403/429 (WAF/throttle) + request exceptions
MANUAL_ATTEMPTS = 5
MANUAL_BACKOFF_BASE = 2.0  # seconds (grows)

EXPECTED_MIN_COLS = 80
MAX_VALIDATION_BYTES = 512 * 1024

DEFAULT_HEADERS = {
    "Referer": "https://openpaymentsdata.cms.gov/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/143.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/csv,application/octet-stream;q=0.9,*/*;q=0.8",
}
# ====================================================


# ---------------------------
# LOGGING (FILE ONLY)
# ---------------------------
def setup_logging(out_root: Path, verbose: bool) -> Path:
    logs_dir = out_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"openpayments_download_2023_{ts}.log"

    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")

    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info("Logging to: %s", log_path.resolve())
    return log_path


# ---------------------------
# UTILS
# ---------------------------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def make_session(pool_size: int) -> requests.Session:
    sess = requests.Session()

    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update(DEFAULT_HEADERS)
    return sess


def parse_slice(slice_str: Optional[str]) -> Optional[slice]:
    """
    "0:10"   -> slice(0, 10)
    "90:-1"  -> slice(90, None)   # -1 means "to end"
    ":50"    -> slice(None, 50)
    "10:"    -> slice(10, None)
    "10"     -> slice(0,10)
    """
    if not slice_str:
        return None

    s = slice_str.strip()
    if ":" not in s:
        try:
            n = int(s)
            return slice(0, n)
        except Exception:
            raise ValueError(f"Invalid --slice '{slice_str}'. Use formats like 0:10, 90:-1, :50, 10:")

    left, right = s.split(":", 1)
    left = left.strip()
    right = right.strip()

    start = int(left) if left != "" else None
    if right == "":
        end = None
    else:
        end_int = int(right)
        end = None if end_int == -1 else end_int

    return slice(start, end)


def truncate_trailing_partial_row(path: Path) -> None:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return
        with path.open("rb+") as fh:
            fh.seek(-1, os.SEEK_END)
            if fh.read(1) == b"\n":
                return
            block = 131072
            pos = fh.tell()
            buffer = b""
            last_nl = -1
            while pos > 0:
                readpos = max(0, pos - block)
                fh.seek(readpos)
                chunk = fh.read(pos - readpos)
                buffer = chunk + buffer
                idx = buffer.rfind(b"\n")
                if idx != -1:
                    last_nl = readpos + idx
                    break
                pos = readpos
            if last_nl == -1:
                fh.truncate(0)
                return
            fh.truncate(last_nl + 1)
    except Exception:
        return


def validate_header_min_cols(path: Path, expected_min_cols: int) -> Tuple[bool, str]:
    try:
        sample = path.read_bytes()[:MAX_VALIDATION_BYTES]
        header_line = None
        for enc in ("utf-8-sig", "utf-8", "cp1252"):
            try:
                text = sample.decode(enc, errors="replace")
                for line in text.splitlines():
                    if line.strip():
                        header_line = line
                        break
                if header_line:
                    break
            except Exception:
                continue

        if not header_line:
            return (False, "empty_or_binary_file")

        cols = next(csv.reader([header_line]))
        if len(cols) < expected_min_cols:
            return (False, f"too_few_columns({len(cols)}<{expected_min_cols})")
        return (True, "")
    except Exception as e:
        return (False, f"validation_error:{e}")


def count_lines_quick(path: Path, max_lines: int = 3) -> int:
    try:
        n = 0
        with path.open("rb") as f:
            for _ in f:
                n += 1
                if n >= max_lines:
                    break
        return n
    except Exception:
        return 0


def build_params(company_id: str, offset: int) -> dict:
    # matches your working sample: NO year condition (dataset itself is 2023)
    return {
        "conditions[0][property]": "applicable_manufacturer_or_applicable_gpo_making_payment_id",
        "conditions[0][operator]": "=",
        "conditions[0][value]": company_id,
        "format": "csv",
        "limit": PAGE_LIMIT,
        "offset": offset,
    }


def request_with_manual_backoff(
    session: requests.Session,
    url: str,
    params: dict,
    timeout: Tuple[int, int],
) -> requests.Response:
    """
    Manual retry loop for WAF/throttle (403/429) + transient exceptions.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, MANUAL_ATTEMPTS + 1):
        try:
            resp = session.get(url, params=params, stream=True, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last_exc = e
            sleep_s = MANUAL_BACKOFF_BASE * (attempt ** 1.3)
            logging.warning("REQUEST EXCEPTION attempt=%s/%s err=%s sleep=%.2fs", attempt, MANUAL_ATTEMPTS, e, sleep_s)
            time.sleep(sleep_s)
            continue

        if resp.status_code in (403, 429):
            sleep_s = MANUAL_BACKOFF_BASE * (attempt ** 1.6)
            logging.warning(
                "HTTP %s attempt=%s/%s (WAF/throttle?) sleep=%.2fs params=%s",
                resp.status_code, attempt, MANUAL_ATTEMPTS, sleep_s, params
            )
            time.sleep(sleep_s)
            continue

        return resp

    if last_exc:
        raise last_exc
    raise RuntimeError("request_with_manual_backoff exhausted without response")


# ---------------------------
# PAGE DOWNLOAD
# ---------------------------
def fetch_page_to_partfile(
    session: requests.Session,
    url: str,
    company_id: str,
    offset: int,
    part_path: Path,
    is_first_page: bool,
) -> Tuple[bool, str, int, int]:
    """
    Returns: (ok, msg, header_lines_written, data_lines_written)
    Header-only => empty_page_no_data_rows
    """
    params = build_params(company_id, offset)
    logging.debug("REQUEST url=%s company_id=%s offset=%s params=%s", url, company_id, offset, params)

    try:
        resp = request_with_manual_backoff(
            session=session,
            url=url,
            params=params,
            timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
        )
    except Exception as e:
        return (False, f"request_error:{e}", 0, 0)

    if resp.status_code != 200:
        return (False, f"HTTP {resp.status_code} at offset {offset}", 0, 0)

    try:
        if part_path.exists():
            part_path.unlink()

        enc = "utf-8-sig" if is_first_page else "utf-8"
        header_lines = 0
        data_lines = 0
        first_nonempty_seen = False

        with part_path.open("w", encoding=enc, newline="") as fh:
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue

                if not first_nonempty_seen:
                    first_nonempty_seen = True
                    if is_first_page:
                        fh.write(line + "\n")
                        header_lines = 1
                    continue  # skip header line for non-first pages

                fh.write(line + "\n")
                data_lines += 1

        truncate_trailing_partial_row(part_path)

        if data_lines == 0:
            try:
                part_path.unlink()
            except Exception:
                pass
            return (True, "empty_page_no_data_rows", header_lines, data_lines)

        return (True, "ok", header_lines, data_lines)

    except Exception as e:
        return (False, f"write_error:{e}", 0, 0)


# ---------------------------
# SMALL: sequential paging
# ---------------------------
def download_small_sequential(
    session: requests.Session,
    url: str,
    company_id: str,
    year_dir: Path,
) -> Tuple[str, int, bool, str]:
    final_path = year_dir / f"csv_{company_id}.csv"
    tmp_path = final_path.with_suffix(".csv.part")

    logging.info("START SMALL company_id=%s -> %s", company_id, final_path)

    if tmp_path.exists():
        try:
            tmp_path.unlink()
        except Exception:
            pass

    offset = 0
    total_data_rows = 0
    wrote_header = False
    page_num = 0

    while True:
        params = build_params(company_id, offset)
        logging.debug("SMALL GET company_id=%s page=%s offset=%s params=%s", company_id, page_num, offset, params)

        try:
            resp = request_with_manual_backoff(
                session=session,
                url=url,
                params=params,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )
        except Exception as e:
            logging.error("SMALL REQUEST ERROR company_id=%s offset=%s err=%s", company_id, offset, e)
            try:
                tmp_path.unlink()
            except Exception:
                pass
            return (company_id, YEAR, False, f"request_error:{e}")

        if resp.status_code != 200:
            logging.error("SMALL HTTP ERROR company_id=%s offset=%s status=%s", company_id, offset, resp.status_code)
            try:
                tmp_path.unlink()
            except Exception:
                pass
            return (company_id, YEAR, False, f"HTTP {resp.status_code} at offset {offset}")

        mode = "w" if offset == 0 else "a"
        enc = "utf-8-sig" if offset == 0 else "utf-8"

        header_seen = False
        data_lines = 0

        with tmp_path.open(mode, encoding=enc, newline="") as fh:
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if not header_seen:
                    header_seen = True
                    if offset == 0:
                        fh.write(line + "\n")
                        wrote_header = True
                    continue
                fh.write(line + "\n")
                data_lines += 1

        truncate_trailing_partial_row(tmp_path)

        logging.debug(
            "SMALL PAGE DONE company_id=%s offset=%s wrote_header=%s data_lines=%s bytes=%s",
            company_id, offset, wrote_header, data_lines, tmp_path.stat().st_size if tmp_path.exists() else 0
        )

        if data_lines == 0:
            break

        total_data_rows += data_lines
        offset += PAGE_LIMIT
        page_num += 1
        time.sleep(0.1)

    if not wrote_header or total_data_rows == 0:
        logging.warning("NO RESULTS (header-only) company_id=%s", company_id)
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return (company_id, YEAR, False, "no_results_header_only")

    ok, reason = validate_header_min_cols(tmp_path, EXPECTED_MIN_COLS)
    if not ok:
        logging.error("VALIDATION FAILED company_id=%s reason=%s", company_id, reason)
        try:
            tmp_path.unlink()
        except Exception:
            pass
        return (company_id, YEAR, False, f"validation_failed:{reason}")

    tmp_path.replace(final_path)
    logging.info("DONE SMALL company_id=%s rows~%s -> %s", company_id, total_data_rows, final_path)
    return (company_id, YEAR, True, f"downloaded_small_ok_rows~{total_data_rows}")


# ---------------------------
# BIG: page-parallel + merge
# ---------------------------
def merge_parts(parts: List[Path], final_path: Path) -> Tuple[bool, str]:
    try:
        if final_path.exists():
            final_path.unlink()
        ensure_dir(final_path.parent)

        with final_path.open("wb") as out:
            for p in parts:
                if p.exists() and p.stat().st_size > 0:
                    out.write(p.read_bytes())

        if not final_path.exists() or final_path.stat().st_size == 0:
            return (False, "merged_empty")

        if count_lines_quick(final_path, max_lines=3) < 2:
            try:
                final_path.unlink()
            except Exception:
                pass
            return (False, "no_results_header_only_after_merge")

        ok, reason = validate_header_min_cols(final_path, EXPECTED_MIN_COLS)
        if not ok:
            try:
                final_path.unlink()
            except Exception:
                pass
            return (False, f"validation_failed:{reason}")

        return (True, "merged_ok")

    except Exception as e:
        return (False, f"merge_error:{e}")


def download_big_parallel_pages(
    session: requests.Session,
    url: str,
    company_id: str,
    expected_total: int,
    year_dir: Path,
    page_workers: int,
) -> Tuple[str, int, bool, str]:
    pages = int(math.ceil(expected_total / PAGE_LIMIT))
    if pages <= 0:
        return (company_id, YEAR, False, "bad_expected_total")

    final_path = year_dir / f"csv_{company_id}.csv"
    parts_root = year_dir / "_parts" / company_id
    ensure_dir(parts_root)

    part_paths = [parts_root / f"part_{i:06d}.csv" for i in range(pages)]
    offsets = [i * PAGE_LIMIT for i in range(pages)]

    logging.info("START BIG company_id=%s expected_total=%s pages=%s -> %s", company_id, expected_total, pages, final_path)

    results: Dict[int, Tuple[bool, str, int, int]] = {}
    total_data_rows = 0

    with ThreadPoolExecutor(max_workers=page_workers) as ex:
        futs = {}
        for i, offset in enumerate(offsets):
            fut = ex.submit(
                fetch_page_to_partfile,
                session,
                url,
                company_id,
                offset,
                part_paths[i],
                i == 0,
            )
            futs[fut] = i

        for fut in as_completed(futs):
            i = futs[fut]
            try:
                ok, msg, header_lines, data_lines = fut.result()
                results[i] = (ok, msg, header_lines, data_lines)
                total_data_rows += data_lines
                logging.debug(
                    "BIG PAGE RESULT company_id=%s page=%s offset=%s ok=%s msg=%s data_lines=%s",
                    company_id, i, offsets[i], ok, msg, data_lines
                )
            except Exception as e:
                results[i] = (False, f"executor_error:{e}", 0, 0)

    hard_fails = [(i, r[1]) for i, r in results.items() if not r[0]]
    if hard_fails:
        logging.error("BIG FAILED company_id=%s hard_fails=%s", company_id, hard_fails[:3])
        return (company_id, YEAR, False, f"page_download_failed:{hard_fails[:3]}")

    if total_data_rows == 0:
        logging.warning("BIG NO RESULTS (all pages empty) company_id=%s", company_id)
        return (company_id, YEAR, False, "no_results_all_pages_empty")

    ok, msg = merge_parts(part_paths, final_path)
    if not ok:
        logging.error("MERGE FAILED company_id=%s msg=%s", company_id, msg)
        return (company_id, YEAR, False, msg)

    # cleanup parts (best-effort)
    try:
        for p in part_paths:
            if p.exists():
                p.unlink()
        if parts_root.exists() and not any(parts_root.iterdir()):
            parts_root.rmdir()
    except Exception:
        pass

    logging.info("DONE BIG company_id=%s rows~%s -> %s", company_id, total_data_rows, final_path)
    return (company_id, YEAR, True, f"downloaded_big_ok_pages={pages}_rows~{total_data_rows}")


# ---------------------------
# INPUT: totals json
# ---------------------------
def load_company_totals_json(path: Path) -> pd.DataFrame:
    df = pd.read_json(path)
    df["company_id"] = df["company_id"].astype(str)
    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--totals-json", required=True, help="Path to openpayments_companies_totals_by_year.json")
    parser.add_argument("--id-workers", type=int, default=10, help="Company-level workers (default: 10)")
    parser.add_argument("--page-workers", type=int, default=10, help="Page-level workers for big IDs (default: 10)")
    parser.add_argument("--out-root", default=".", help="Output root folder (default: current folder)")
    parser.add_argument("--slice", default=None, help='Optional slicing like "0:10" or "90:-1". If omitted, runs all.')
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging to file")
    args = parser.parse_args()

    out_root = Path(args.out_root)
    setup_logging(out_root=out_root, verbose=args.verbose)

    totals_path = Path(args.totals_json)
    if not totals_path.exists():
        raise FileNotFoundError(f"Totals JSON not found: {totals_path}")

    df = load_company_totals_json(totals_path)

    year_dir = out_root / str(YEAR)
    ensure_dir(year_dir)

    total_col = f"total_{YEAR}"
    if total_col not in df.columns:
        logging.error("Totals JSON missing required column: %s", total_col)
        return

    temp = df[["company_id", total_col]].copy()
    temp[total_col] = temp[total_col].fillna(0).astype(int)
    temp = temp[temp[total_col] > 0]  # skip zeros
    temp = temp.sort_values("company_id")

    tasks: List[Tuple[str, int]] = [(row["company_id"], int(row[total_col])) for _, row in temp.iterrows()]

    if not tasks:
        logging.warning("No tasks found (maybe total_2023 is 0 for all rows).")
        return

    sl = parse_slice(args.slice)
    if sl is not None:
        tasks = tasks[sl]
        logging.info("Applied slice=%s -> tasks=%d", args.slice, len(tasks))

    if not tasks:
        logging.warning("No tasks left after slicing.")
        return

    logging.info("Total tasks to run (2023 only): %d", len(tasks))
    logging.info("Dataset URL (2023): %s", BASE_URL_2023)
    logging.info("id_workers=%s | page_workers=%s | page_limit=%s", args.id_workers, args.page_workers, PAGE_LIMIT)

    pool_size = max(args.id_workers, args.page_workers) * 3
    session = make_session(pool_size=pool_size)

    results = []
    with ThreadPoolExecutor(max_workers=args.id_workers) as ex:
        futs = {}

        for company_id, expected_total in tasks:
            logging.info("QUEUE company_id=%s expected_total=%s", company_id, expected_total)

            if expected_total <= PAGE_LIMIT:
                fut = ex.submit(download_small_sequential, session, BASE_URL_2023, company_id, year_dir)
            else:
                fut = ex.submit(download_big_parallel_pages, session, BASE_URL_2023, company_id, expected_total, year_dir, args.page_workers)

            futs[fut] = (company_id, expected_total)

        for fut in tqdm(as_completed(futs), total=len(futs), desc="Downloading (2023)", unit="job"):
            company_id, expected_total = futs[fut]
            try:
                cid, y, ok, msg = fut.result()
                results.append((cid, y, expected_total, ok, msg))
                if ok:
                    logging.info("SUCCESS company_id=%s total=%s msg=%s", cid, expected_total, msg)
                else:
                    logging.warning("FAIL company_id=%s total=%s msg=%s", cid, expected_total, msg)
            except Exception as e:
                results.append((company_id, YEAR, expected_total, False, f"executor_error:{e}"))
                logging.exception("FAIL (exception) company_id=%s total=%s", company_id, expected_total)

    ok_count = sum(1 for _, _, _, ok, _ in results if ok)
    fail_count = len(results) - ok_count

    logging.info("DONE. Success: %d/%d | Failed: %d/%d", ok_count, len(results), fail_count, len(results))

    report_path = out_root / "download_report_2023.csv"
    rep_df = pd.DataFrame(results, columns=["company_id", "year", "expected_total", "ok", "message"])
    rep_df.to_csv(report_path, index=False)
    logging.info("Report saved: %s", report_path.resolve())


if __name__ == "__main__":
    main()
