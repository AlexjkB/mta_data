# MTA Data Pipeline — Usage Guide

## Prerequisites

- Python 3.10+
- Terraform
- Docker and Docker Compose
- GCP service account key with BigQuery Admin and Storage Admin roles

## Setup

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

Create a `.env` file in the project root:

```
GCS_BUCKET=mta-data-491403-bucket
BQ_DATASET=mta_warehouse
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

Create a `docker/.env` file (for Airflow):

```
GCS_BUCKET=mta-data-491403-bucket
BQ_DATASET=mta_warehouse
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
```

### 3. Provision infrastructure (first time only)

Create a `terraform/terraform.tfvars` file:

```
credentials_file = "/path/to/your/service-account-key.json"
```

Then run:

```bash
cd terraform
terraform init
terraform apply
cd ..
```

This creates:
- GCS bucket (data lake for raw Parquet files)
- BigQuery dataset (mta_warehouse)

## Extract & Load (Manual)

### Extract data from SODA API to GCS

```bash
source .venv/bin/activate

# Full historical load (all data)
python -m pipelines.extract --dataset daily_ridership

# Load from a specific date onward
python -m pipelines.extract --dataset subway_hourly --start-date 2024-01-01

# Available datasets:
#   daily_ridership   - daily ridership totals (small, ~1.8k rows)
#   subway_hourly     - hourly subway ridership by station (large, ~27M rows from 2024)
#   bus_hourly        - hourly bus ridership by route (large, ~55M rows from 2024)
#   bridges_tunnels   - daily bridge/tunnel traffic (small, ~3.7k rows from 2024)
#   stations          - reference table of station complexes (445 rows)
```

### Load data from GCS to BigQuery

```bash
python -m pipelines.load --dataset daily_ridership
python -m pipelines.load --dataset subway_hourly
python -m pipelines.load --dataset bus_hourly
python -m pipelines.load --dataset bridges_tunnels
python -m pipelines.load --dataset stations
```

This creates raw tables in BigQuery:
- `mta_warehouse.raw_daily_ridership`
- `mta_warehouse.raw_subway_hourly`
- `mta_warehouse.raw_bus_hourly`
- `mta_warehouse.raw_bridges_tunnels`
- `mta_warehouse.raw_stations`

## dbt Transformations

### Install dbt packages

```bash
cd dbt_mta
dbt deps
```

### Run all models

```bash
dbt run
```

### Run a specific model

```bash
dbt run --select stg_daily_ridership
dbt run --select fct_daily_ridership+   # model and all downstream
```

### Model layers

| Layer | Models | Materialization |
|---|---|---|
| Staging | stg_daily_ridership, stg_subway_hourly, stg_bus_hourly, stg_bridges_tunnels, stg_stations | view |
| Intermediate | int_ridership_daily_by_mode, int_subway_peak_hours, int_station_daily_totals | view |
| Marts | fct_daily_ridership, fct_hourly_station_ridership, fct_monthly_trends, dim_calendar, dim_stations | table |

### Data flow

```
raw_* tables (BigQuery)
  -> stg_* views (clean, cast types, rename columns)
    -> int_* views (join, aggregate, unpivot)
      -> fct_*/dim_* tables (dashboard-ready)
```

## Airflow Orchestration

### Start Airflow

```bash
cd docker
sudo docker-compose up airflow-init   # First time only — initializes DB and admin user
sudo docker-compose up -d             # Start webserver + scheduler in background
```

### Access Airflow UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### DAG: mta_daily_pipeline

Runs daily at 8 AM UTC. Task flow:

```
extract (5 datasets in parallel) -> load to BigQuery -> dbt run -> dbt test
```

### Manage Airflow

```bash
cd docker

# View running containers
sudo docker-compose ps

# View logs
sudo docker-compose logs -f airflow-scheduler
sudo docker-compose logs -f airflow-webserver

# Stop Airflow
sudo docker-compose down

# Rebuild after code changes
sudo docker-compose up --build -d
```

## Project Structure

```
mta_data/
├── .env                        # Environment variables (not in git)
├── .gitignore
├── requirements.txt            # Python dependencies
├── usage.md                    # This file
├── terraform/
│   ├── main.tf                 # GCS bucket + BigQuery dataset
│   ├── variables.tf            # Project ID, region, bucket name
│   ├── outputs.tf              # Output values
│   └── terraform.tfvars        # Credentials (not in git)
├── docker/
│   ├── Dockerfile              # Airflow image with Python deps + dbt
│   ├── docker-compose.yml      # Airflow webserver, scheduler, postgres
│   └── .env                    # Docker environment variables (not in git)
├── pipelines/
│   ├── __init__.py
│   ├── config.py               # Dataset definitions, API config
│   ├── extract.py              # SODA API -> Parquet -> GCS
│   └── load.py                 # GCS Parquet -> BigQuery raw tables
├── airflow/
│   └── dags/
│       └── mta_daily_pipeline.py  # Daily pipeline DAG
└── dbt_mta/
    ├── dbt_project.yml
    ├── profiles.yml            # BigQuery connection config
    ├── packages.yml            # dbt-utils
    └── models/
        ├── sources.yml         # Raw table source definitions
        ├── staging/            # Clean + cast raw data
        ├── intermediate/       # Business logic joins
        └── marts/              # Final dashboard tables
```
