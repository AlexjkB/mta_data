https://lookerstudio.google.com/reporting/e40dbe2d-7ab6-4626-9772-62025b07ab25

# MTA Data Pipeline

An end-to-end batch ELT pipeline that ingests NYC Metropolitan Transportation Authority (MTA) ridership and traffic data, transforms it with dbt, warehouses it in BigQuery, and surfaces insights in a Looker Studio dashboard.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION (Airflow)                      │
│           Scheduled daily · Retry logic · Task groups               │
└────────┬──────────────┬──────────────────┬──────────────────────────┘
         │              │                  │
         ▼              ▼                  ▼
┌────────────┐  ┌──────────────┐  ┌────────────────┐
│  EXTRACT   │  │   LOAD       │  │   TRANSFORM    │
│            │  │              │  │                │
│ Python     │  │ GCS Parquet  │  │  dbt models    │
│ requests   │  │ → BigQuery   │  │  (BigQuery)    │
│ to SODA    │  │ raw tables   │  │                │
│ API        │  │              │  │  staging →     │
│            │  │              │  │  intermediate →│
│ → Parquet  │  │              │  │  marts         │
│   on GCS   │  │              │  │                │
└────────────┘  └──────────────┘  └───────┬────────┘
                                          │
                                          ▼
                                 ┌────────────────┐
                                 │  SERVE          │
                                 │                │
                                 │ Looker Studio  │
                                 │ dashboard      │
                                 └────────────────┘
```

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.10+ |
| Extraction | `requests` (Socrata SODA API) |
| File Format | Parquet (`pyarrow`) |
| Data Lake | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt-core + dbt-bigquery |
| Orchestration | Apache Airflow (Docker) |
| Infrastructure | Terraform |
| Containerization | Docker + Docker Compose |
| Visualization | Looker Studio |

## Datasets

| Dataset | Source | Records | Date Range |
|---|---|---|---|
| Daily Ridership | MTA systemwide totals | ~1.8k | Mar 2020 – Present |
| Subway Hourly | Ridership by station/hour | ~29M | Jan 2024 – Dec 2024 |
| Bus Hourly | Ridership by route/hour | ~55M | Jan 2024 – Dec 2024 |
| Bridges & Tunnels | Traffic by plaza | ~3.7k | Jan 2024 – Dec 2024 |
| Stations | Reference table | 445 | N/A |

All data sourced from [data.ny.gov](https://data.ny.gov) via the Socrata SODA API.

## dbt Model Layers

```
raw_* tables (BigQuery)
  → stg_* views (clean, cast types, rename columns)
    → int_* views (join, aggregate, unpivot)
      → fct_*/dim_* tables (dashboard-ready)
```

**Staging:** stg_daily_ridership, stg_subway_hourly, stg_bus_hourly, stg_bridges_tunnels, stg_stations

**Intermediate:** int_ridership_daily_by_mode, int_subway_peak_hours, int_station_daily_totals

**Marts:** fct_daily_ridership, fct_hourly_station_ridership, fct_monthly_trends, dim_calendar, dim_stations

## Getting Started

### Prerequisites

- Python 3.10+
- Terraform
- Docker and Docker Compose
- GCP account with a service account key (BigQuery Admin + Storage Admin roles)

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/mta-data-pipeline.git
cd mta-data-pipeline

cp .env.example .env
# Edit .env with your values
```

### 2. Provision infrastructure

```bash
# Create terraform/terraform.tfvars with your credentials path
cd terraform
terraform init
terraform apply
cd ..
```

### 3. Extract and load data

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Extract all datasets
python -m pipelines.extract --dataset daily_ridership
python -m pipelines.extract --dataset subway_hourly --start-date 2024-01-01
python -m pipelines.extract --dataset bus_hourly --start-date 2024-01-01
python -m pipelines.extract --dataset bridges_tunnels --start-date 2024-01-01
python -m pipelines.extract --dataset stations

# Load into BigQuery
python -m pipelines.load --dataset daily_ridership
python -m pipelines.load --dataset subway_hourly
python -m pipelines.load --dataset bus_hourly
python -m pipelines.load --dataset bridges_tunnels
python -m pipelines.load --dataset stations
```

### 4. Run dbt transformations

```bash
cd dbt_mta
dbt deps
dbt run
cd ..
```

### 5. Start Airflow (automated daily runs)

```bash
# Create docker/.env with your credentials
cd docker
sudo docker-compose up airflow-init
sudo docker-compose up -d
```

Airflow UI: http://localhost:8080 (admin/admin)

## Project Structure

```
mta_data/
├── .env.example                # Environment variable template
├── .gitignore
├── requirements.txt            # Python dependencies
├── README.md
├── usage.md                    # Detailed usage guide
├── terraform/
│   ├── main.tf                 # GCS bucket + BigQuery dataset
│   ├── variables.tf            # Project ID, region, bucket name
│   └── outputs.tf              # Output values
├── docker/
│   ├── Dockerfile              # Airflow image with deps + dbt
│   └── docker-compose.yml      # Airflow webserver, scheduler, postgres
├── pipelines/
│   ├── config.py               # Dataset definitions, API config
│   ├── extract.py              # SODA API → Parquet → GCS
│   └── load.py                 # GCS Parquet → BigQuery raw tables
├── airflow/
│   └── dags/
│       └── mta_daily_pipeline.py
└── dbt_mta/
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    └── models/
        ├── sources.yml
        ├── staging/
        ├── intermediate/
        └── marts/
```
