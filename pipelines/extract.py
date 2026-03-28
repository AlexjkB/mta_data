import argparse
import time
from datetime import datetime

import pandas as pd
import requests
from google.cloud import storage

from pipelines.config import DATASETS, GCS_BUCKET, PAGE_SIZE, SODA_BASE_URL

MAX_RETRIES = 5


def generate_month_ranges(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Generate (start, end) pairs for each month in the range."""
    from dateutil.relativedelta import relativedelta

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []

    current = start.replace(day=1)
    while current < end:
        next_month = current + relativedelta(months=1)
        month_end = min(next_month, end)
        ranges.append((current.strftime("%Y-%m-%d"), month_end.strftime("%Y-%m-%d")))
        current = next_month

    return ranges


def api_request_with_retry(url: str, params: dict) -> list:
    """Make an API request with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=300)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
            wait = 2 ** attempt * 5  # 5, 10, 20, 40, 80 seconds
            print(f"    Retry {attempt + 1}/{MAX_RETRIES} after {wait}s — {e}", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def extract_month(dataset_name: str, config: dict, month_start: str, month_end: str,
                  bucket, date_partition: str, chunk_num: int) -> tuple[int, int]:
    """Extract one month of data using offset pagination within that month."""
    offset = 0
    month_records = 0
    date_col = config["date_column"]

    while True:
        where = f"{date_col} >= '{month_start}' AND {date_col} < '{month_end}'"
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$order": date_col,
            "$where": where,
        }

        url = f"{SODA_BASE_URL}/{config['dataset_id']}.json"
        records = api_request_with_retry(url, params)

        if not records:
            break

        df = pd.DataFrame(records)
        gcs_path = f"raw/{dataset_name}/{date_partition}/chunk_{chunk_num:04d}.parquet"
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(df.to_parquet(index=False), content_type="application/octet-stream")

        month_records += len(records)
        chunk_num += 1
        offset += PAGE_SIZE
        print(f"  Chunk {chunk_num}: {month_start} — {month_records:,} this month, uploaded {len(records)} records", flush=True)

    return month_records, chunk_num


def get_existing_chunk_count(bucket, dataset_name: str, date_partition: str) -> int:
    """Count how many chunks already exist in GCS for resuming."""
    blobs = list(bucket.list_blobs(prefix=f"raw/{dataset_name}/{date_partition}/"))
    return len(blobs)


def extract_dataset(dataset_name: str, start_date: str | None = None) -> int:
    config = DATASETS[dataset_name]
    date_partition = datetime.now().strftime("%Y-%m-%d")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    total_records = 0
    chunk_num = 0

    # For datasets without a date column (reference tables), use simple offset pagination
    if not config["date_column"]:
        offset = 0
        while True:
            params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}
            url = f"{SODA_BASE_URL}/{config['dataset_id']}.json"
            records = api_request_with_retry(url, params)

            if not records:
                break

            df = pd.DataFrame(records)
            gcs_path = f"raw/{dataset_name}/{date_partition}/chunk_{chunk_num:04d}.parquet"
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(df.to_parquet(index=False), content_type="application/octet-stream")

            total_records += len(records)
            chunk_num += 1
            offset += PAGE_SIZE
            print(f"  Chunk {chunk_num}: uploaded {len(records)} records ({total_records:,} total)", flush=True)

        print(f"  Uploaded {chunk_num} chunks to gs://{GCS_BUCKET}/raw/{dataset_name}/{date_partition}/")
        return total_records

    # For dated datasets, paginate by month
    if not start_date:
        start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    months = generate_month_ranges(start_date, end_date)

    # Check for existing data to resume
    existing_chunks = get_existing_chunk_count(bucket, dataset_name, date_partition)
    if existing_chunks > 0:
        chunk_num = existing_chunks
        # Estimate which month to resume from (~40 chunks per month)
        skip_months = existing_chunks // 44
        if skip_months > 0 and skip_months < len(months):
            skipped_records = existing_chunks * PAGE_SIZE
            total_records = skipped_records
            months = months[skip_months:]
            print(f"  Resuming: found {existing_chunks} existing chunks, skipping ~{skip_months} months ({skipped_records:,} records)", flush=True)

    print(f"  Extracting {len(months)} months of data...", flush=True)

    for month_start, month_end in months:
        print(f"  Month: {month_start}...", flush=True)
        month_records, chunk_num = extract_month(
            dataset_name, config, month_start, month_end, bucket, date_partition, chunk_num
        )
        total_records += month_records

    print(f"  Uploaded {chunk_num} chunks to gs://{GCS_BUCKET}/raw/{dataset_name}/{date_partition}/")
    return total_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=list(DATASETS.keys()))
    parser.add_argument("--start-date", default=None, help="Incremental start date (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"Extracting {args.dataset}...", flush=True)
    count = extract_dataset(args.dataset, args.start_date)
    print(f"Done. {count:,} records extracted.")
