import argparse
import csv
import os
import re
import time
from datetime import datetime

import pandas as pd

# -- Import everything from match_scraper (must be on PYTHONPATH / same dir) --
from match_scraper import (
    build_driver,
    scrape_match,
    jitter_sleep,
    extract_match_id,
    COLUMN_ORDER,
    LOG_COLUMNS,
    SCRAPED_DATA_DIR,
    SCRAPED_LOGS_DIR,
)

# -- Retry log columns: all original columns + 4 extra at the end -------------
RETRY_LOG_COLUMNS = LOG_COLUMNS + [
    "file_name",      # which data CSV was patched
    "fields_before",  # non_empty_fields in the original log row
    "fields_after",   # non_empty_fields after re-scrape
    "row_updated",    # True/False - was the data CSV row actually overwritten
    "retry_trigger",  # what triggered the retry: low_fields / no_lineups / no_stats
    "data_csv_found", # whether the data CSV existed at retry time
]

# --- Constants ----------------------------------------------------------------

DEFAULT_THRESHOLD = 30   # re-scrape rows with non_empty_fields < this value (strict)

# Reasons that also trigger a retry regardless of field count
RETRY_REASONS = {"no_lineups", "no_stats"}


# --- Helpers ------------------------------------------------------------------

def log_stem(log_path: str) -> str:
    """
    Extract the base stem from a log file path.
    e.g. "scraped_logs/spain_laliga_2024-2025_scrape_log.csv"
         -> "spain_laliga_2024-2025"
    """
    base = os.path.basename(log_path)
    stem = re.sub(r"_(scrape|retry)_log.*\.csv$", "", base, flags=re.IGNORECASE)
    return stem


def derive_data_path(log_path: str) -> str:
    """
    Given a log CSV path, return the expected matching data CSV path.
    scraped_logs/<stem>_scrape_log.csv -> scraped_data/<stem>_match_data.csv
    """
    stem = log_stem(log_path)
    return os.path.join(SCRAPED_DATA_DIR, f"{stem}_match_data.csv")


def read_low_field_rows(log_path: str, threshold: int) -> list[dict]:
    """
    Return all rows from the log CSV that need a retry:
      - non_empty_fields <= threshold, OR
      - reason is in RETRY_REASONS (e.g. "no_lineups")
    """
    low_rows = []
    seen_urls = set()
    with open(log_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url", "").strip()
            reason = row.get("reason", "").strip().lower()
            # Avoid duplicates if a URL appears multiple times in the log
            if url in seen_urls:
                continue
            if reason in RETRY_REASONS:
                low_rows.append(row)
                seen_urls.add(url)
                continue
            raw = row.get("non_empty_fields", "").strip()
            try:
                if int(raw) < threshold:
                    low_rows.append(row)
                    seen_urls.add(url)
            except ValueError:
                pass   # skip rows with blank/invalid field count
    return low_rows


def append_log(retry_log_path: str, row: dict) -> None:
    """
    Append one row to the unified retry log CSV.
    Uses RETRY_LOG_COLUMNS (original LOG_COLUMNS + 4 extra fields).
    Creates headers automatically if the file is new.
    """
    file_exists = os.path.isfile(retry_log_path)
    with open(retry_log_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RETRY_LOG_COLUMNS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def patch_data_csv(data_path: str, match_id: str, new_data: dict) -> bool:
    """
    Find the row in data_path where the match_id column equals match_id
    (extracted from the mid= param in the URL), overwrite ONLY that row
    with new_data, and leave all other rows completely untouched.
    Returns True if the row was found and patched, False otherwise.
    """
    if not os.path.isfile(data_path):
        print(f"  WARNING: Data CSV not found: {data_path}")
        return False

    df = pd.read_csv(data_path, dtype=str).fillna("")

    mask = df["match_id"] == match_id
    if not mask.any():
        print(f"  WARNING: match_id '{match_id}' not found in {data_path}")
        return False

    idx = df.index[mask][0]

    new_series = pd.Series({col: str(new_data.get(col, "")) for col in COLUMN_ORDER})
    df.loc[idx, [c for c in COLUMN_ORDER if c in df.columns]] = \
        new_series[[c for c in COLUMN_ORDER if c in df.columns]]

    df.to_csv(data_path, index=False)
    return True


# --- Core retry logic for one log file ---------------------------------------

def retry_log_file(
    log_path: str,
    driver,
    retry_log_path: str,
    cumulative_total: float,
    threshold: int = DEFAULT_THRESHOLD,
    debug: bool = False,
) -> float:
    """
    Process one scrape-log CSV:
      1. Find rows with non_empty_fields <= threshold.
      2. Re-scrape each URL.
      3. Patch the data CSV row only if the new scrape has MORE non-empty fields.
      4. Append each result to the shared unified retry log (same format as original).

    Returns the updated cumulative_total so it carries across log files.
    """
    print(f"\n{'='*60}")
    print(f"  Log file : {log_path}")

    if not os.path.isfile(log_path):
        print(f"  FAILED: Log file not found - skipping.")
        return cumulative_total

    data_path = derive_data_path(log_path)
    print(f"  Data CSV : {data_path}")

    if not os.path.isfile(data_path):
        print(f"  WARNING: Data CSV not found: {data_path}")
        print(f"  WARNING: Will still re-scrape and log, but cannot patch rows.")

    # -- Find candidate rows ---------------------------------------------------
    low_rows = read_low_field_rows(log_path, threshold)
    total    = len(low_rows)

    if total == 0:
        print(f"  OK: No rows qualifying for retry (fields < {threshold}, no_stats, no_lineups) - nothing to do.")
        print(f"{'='*60}")
        return cumulative_total

    print(f"  Found {total} row(s) qualifying for retry.")
    print(f"{'='*60}")

    for i, orig_row in enumerate(low_rows, start=1):
        url           = orig_row.get("url", "").strip()
        fields_before = orig_row.get("non_empty_fields", "0").strip()
        match_id      = extract_match_id(url)

        reason = orig_row.get("reason", "").strip().lower()

        print(f"\n{'-'*60}")
        print(f"  [{i}/{total}] URL          : {url}")
        print(f"              match_id     : {match_id or '(not found)'}")
        print(f"              fields_before: {fields_before}")
        print(f"{'-'*60}")

        # -- Build log row skeleton (original LOG_COLUMNS + 4 extra fields) ----
        log_row = {col: "" for col in RETRY_LOG_COLUMNS}
        log_row["scraped_at"]    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_row["data_csv_found"] = os.path.isfile(data_path)
        log_row["url"]           = url
        log_row["success"]       = False
        log_row["file_name"]     = os.path.basename(data_path)
        log_row["fields_before"] = fields_before
        log_row["row_updated"]   = False

        if reason in RETRY_REASONS:
            log_row["retry_trigger"] = reason
        else:
            log_row["retry_trigger"] = "low_fields"

        try:
            new_data, timing = scrape_match(url, driver=driver, debug=debug)

            fields_after = timing["non_empty_fields"]
            print(f"\n  fields_before={fields_before}  ->  fields_after={fields_after}")

            # -- Decide whether to patch the data CSV --------------------------
            try:
                before_int = int(fields_before)
            except ValueError:
                before_int = 0

            patched = False
            if new_data and fields_after > before_int:
                if match_id:
                    patched = patch_data_csv(data_path, match_id, new_data)
                    if patched:
                        print(f"  OK: Row updated in {os.path.basename(data_path)}")
                    else:
                        print(f"  WARNING: Patch failed (match_id not found or data CSV missing).")
                else:
                    print(f"  WARNING: Cannot patch - match_id could not be extracted from URL.")
            elif not new_data:
                print(f"  FAILED: Scrape returned no data.")
            else:
                print(f"  SKIP: New scrape ({fields_after}) <= original ({before_int}) - row NOT updated.")

            # -- Fill timing columns + extra retry fields ---------------------
            cumulative_total += timing["t_total_s"]
            log_row.update({
                "success":               timing.get("success", False),
                "reason":                timing.get("reason") or "none",
                "non_empty_fields":      fields_after,
                "fields_after":          fields_after,
                "row_updated":           patched if (new_data and fields_after > before_int and match_id) else False,
                "t_summary_s":           timing["t_summary_s"],
                "sleep_after_summary_s": timing["sleep_after_summary_s"],
                "t_stats_s":             timing["t_stats_s"],
                "sleep_after_stats_s":   timing["sleep_after_stats_s"],
                "t_lineups_s":           timing["t_lineups_s"],
                "t_scrape_only_s":       timing["t_scrape_only_s"],
                "t_total_s":             timing["t_total_s"],
                "cumulative_total_s":    round(cumulative_total, 2),
            })

            print(f"  Summary : {timing['t_summary_s']}s  "
                  f"(+{timing['sleep_after_summary_s']}s sleep)")
            print(f"  Stats   : {timing['t_stats_s']}s  "
                  f"(+{timing['sleep_after_stats_s']}s sleep)")
            print(f"  Lineups : {timing['t_lineups_s']}s")
            print(f"  Total   : {timing['t_total_s']}s  "
                  f"(cumulative {cumulative_total:.2f}s / {cumulative_total/60:.1f}min)")

        except Exception as e:
            print(f"\n  FAILED: ERROR scraping {url}: {e}")
            log_row["success"]            = False
            log_row["reason"]             = str(e)[:120]
            log_row["cumulative_total_s"] = round(cumulative_total, 2)

        # -- Sleep between matches (skip after last match in this file) --------
        sleep_between = 0.0
        if i < total:
            sleep_between = jitter_sleep(2.0, 4.0)
            print(f"  Sleeping {sleep_between:.1f}s before next match...")

        log_row["sleep_before_next_match_s"] = round(sleep_between, 2)
        cumulative_total += sleep_between
        log_row["cumulative_total_s"] = round(cumulative_total, 2)

        append_log(retry_log_path, log_row)

    return cumulative_total


# --- CLI ----------------------------------------------------------------------

def collect_log_files(paths: list[str]) -> list[str]:
    """Deduplicate and resolve to absolute paths."""
    seen, result = set(), []
    for p in paths:
        abs_p = os.path.abspath(p)
        if abs_p not in seen:
            seen.add(abs_p)
            result.append(abs_p)
    return result


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Re-scrape matches whose log rows have <= N non-empty fields, "
            "patch the data CSV in-place, and write one unified retry log."
        )
    )

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--log",
        nargs="+",
        metavar="LOG_CSV",
        help="One or more *_scrape_log.csv files to process.",
    )
    src.add_argument(
        "--dir",
        metavar="DIR",
        help=(
            "Directory to scan for *_scrape_log.csv files. "
            "Existing *_retry_log*.csv files are skipped automatically."
        ),
    )

    parser.add_argument(
        "--threshold",
        type=int,
        default=DEFAULT_THRESHOLD,
        metavar="N",
        help=f"Re-scrape rows with non_empty_fields < N (default: {DEFAULT_THRESHOLD}). Always also retries no_stats and no_lineups regardless of field count.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Save raw HTML debug files for each scrape.",
    )
    args = parser.parse_args()

    # -- Collect log files -----------------------------------------------------
    log_files: list[str] = []

    if args.dir:
        directory = os.path.abspath(args.dir)
        if not os.path.isdir(directory):
            parser.error(f"--dir is not a valid directory: {directory}")

        found = sorted(
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.lower().endswith("_scrape_log.csv")   # skips _retry_log*.csv
        )
        if not found:
            print(f"  No *_scrape_log.csv files found in: {directory}")
            return
        log_files.extend(found)

    if args.log:
        for lp in args.log:
            if not os.path.isfile(lp):
                parser.error(f"--log file does not exist: {lp}")
        log_files.extend(args.log)

    log_files = collect_log_files(log_files)

    # -- One persistent unified retry log saved next to this script -----------
    # Appended to on every run; created automatically if it doesn't exist yet.
    script_dir     = os.path.dirname(os.path.abspath(__file__))
    retry_log_path = os.path.join(script_dir, "scraping_failed_urls_log.csv")

    print(f"\n  Threshold  : <= {args.threshold} non-empty fields")
    print(f"  Log files  : {len(log_files)}")
    for lf in log_files:
        print(f"    * {lf}")
    print(f"  Retry log  : {retry_log_path}")

    # -- Build driver once for the entire run ----------------------------------
    driver           = build_driver()
    grand_start      = time.perf_counter()
    cumulative_total = 0.0

    try:
        for i, lf in enumerate(log_files):
            cumulative_total = retry_log_file(
                log_path         = lf,
                driver           = driver,
                retry_log_path   = retry_log_path,
                cumulative_total = cumulative_total,
                threshold        = args.threshold,
                debug            = args.debug,
            )
            # Sleep between log files (not after the last one)
            if i < len(log_files) - 1:
                sleep = jitter_sleep(30.0, 60.0)
                print(f"  Sleeping {sleep:.1f}s before next log file...")
                cumulative_total += sleep

    finally:
        driver.quit()
        grand_wall = time.perf_counter() - grand_start
        print(f"\n  Chrome closed.")
        print(f"  Retry log (persistent) -> {retry_log_path}")
        print(f"  Grand total     : {grand_wall:.1f}s ({grand_wall/60:.1f} min)\n")


if __name__ == "__main__":
    main()
