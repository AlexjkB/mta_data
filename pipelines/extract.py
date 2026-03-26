import argparse
from datetime import datetime

import pandas as pd
import requests
from google.cloud import storage

from pipelines.config import DATASETS, GCS_BUCKET, PAGE_SIZE, SODA_BASE_URL


def extract_dataset(dataset_name: str, start_date: str | None = None) -> int:
    config = DATASETS[dataset_name]
    offset = 0
    total_records = 0
    chunk_num = 0
    date_partition = datetime.now().strftime("%Y-%m-%d")
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset, "$order": ":id"}
        if start_date and config["date_column"]:
            params["$where"] = f"{config['date_column']} >= '{start_date}'"

        url = f"{SODA_BASE_URL}/{config['dataset_id']}.json"
        response = requests.get(url, params=params)
        response.raise_for_status()
        records = response.json()

        if not records:
            break

        df = pd.DataFrame(records)
        gcs_path = f"raw/{dataset_name}/{date_partition}/chunk_{chunk_num:04d}.parquet"
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(df.to_parquet(index=False), content_type="application/octet-stream")

        total_records += len(records)
        chunk_num += 1
        offset += PAGE_SIZE
        print(f"  Chunk {chunk_num}: uploaded {len(records)} records ({total_records} total)")

    if total_records == 0:
        print(f"  No records found for {dataset_name}")

    print(f"  Uploaded {chunk_num} chunks to gs://{GCS_BUCKET}/raw/{dataset_name}/{date_partition}/")
    return total_records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=list(DATASETS.keys()))
    parser.add_argument("--start-date", default=None, help="Incremental start date (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"Extracting {args.dataset}...")
    count = extract_dataset(args.dataset, args.start_date)
    print(f"Done. {count} records extracted.")
