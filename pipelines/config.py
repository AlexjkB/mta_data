import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DATASETS = {
    "daily_ridership": {
        "dataset_id": "vxuj-8kew",
        "date_column": "date",
        "bq_table": "raw_daily_ridership",
    },
    "subway_hourly": {
        "dataset_id": "wujg-7c2s",
        "date_column": "transit_timestamp",
        "bq_table": "raw_subway_hourly",
    },
    "bus_hourly": {
        "dataset_id": "kv7t-n8in",
        "date_column": "transit_timestamp",
        "bq_table": "raw_bus_hourly",
    },
    "bridges_tunnels": {
        "dataset_id": "dtj7-qync",
        "date_column": "collection_date",
        "bq_table": "raw_bridges_tunnels",
    },
    "stations": {
        "dataset_id": "5f5g-n3cz",
        "date_column": None,  # reference table, no date filter
        "bq_table": "raw_stations",
    },
}

SODA_BASE_URL = "https://data.ny.gov/resource"
GCS_BUCKET = os.environ["GCS_BUCKET"]
BQ_DATASET = os.environ["BQ_DATASET"]
PAGE_SIZE = 50000
