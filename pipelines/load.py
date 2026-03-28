import argparse

from google.cloud import bigquery, storage

from pipelines.config import BQ_DATASET, DATASETS, GCS_BUCKET


def get_parquet_uris(dataset_name: str) -> list[str]:
    """List all parquet file URIs for a dataset in GCS."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=f"raw/{dataset_name}/")
    return [f"gs://{GCS_BUCKET}/{b.name}" for b in blobs if b.name.endswith(".parquet")]


def load_to_bigquery(dataset_name: str):
    config = DATASETS[dataset_name]
    client = bigquery.Client()

    uris = get_parquet_uris(dataset_name)
    if not uris:
        print(f"  No parquet files found for {dataset_name}")
        return

    table_ref = f"{client.project}.{BQ_DATASET}.{config['bq_table']}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"  Loading {len(uris)} files → {table_ref}...")
    load_job = client.load_table_from_uri(uris, table_ref, job_config=job_config)
    load_job.result()

    table = client.get_table(table_ref)
    print(f"  Loaded {table.num_rows:,} rows into {table_ref}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, choices=list(DATASETS.keys()))
    args = parser.parse_args()

    print(f"Loading {args.dataset} into BigQuery...")
    load_to_bigquery(args.dataset)
    print("Done.")
