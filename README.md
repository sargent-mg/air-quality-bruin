# Mexico Air Quality Monitoring Pipeline — Bruin

## Problem Description

Air pollution is one of the leading causes of premature death in Mexico, with northern industrial cities like Chihuahua, Juárez, and Monterrey regularly exceeding WHO guideline levels for particulate matter and gaseous pollutants. Despite government monitoring stations collecting hourly readings across the country, this data is scattered across raw archives and APIs with inconsistent formats, units, and coverage gaps — making it difficult for researchers, journalists, or policymakers to get a clear picture of air quality trends.

This project builds an end-to-end data pipeline that:

1. **Ingests** air quality data from 300 monitoring stations across 121 Mexican cities, covering 12 pollutant types (PM2.5, PM10, O3, NO2, CO, SO2, NO, NOx, PM1, temperature, humidity, particle counts).
2. **Cleans and normalizes** the data — deduplicating readings, converting inconsistent measurement units (ppm → µg/m³), and filtering sensor errors.
3. **Transforms** the data into analytics-ready tables: a city-level daily Air Quality Index (AQI) fact table with WHO guideline exceedance flags, and a station reliability dimension.
4. **Serves** an interactive Looker Studio dashboard showing pollution hotspots on a map, worst-city rankings, and daily trend lines.

The pipeline supports both **batch** ingestion (historical data from the OpenAQ S3 archive) and **streaming** ingestion (near-real-time readings via Kafka/Redpanda), demonstrating both patterns in a single project.

**Data source:** [OpenAQ](https://openaq.org) — the world's largest open-source air quality data platform, aggregating government-grade measurements from 130+ countries.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          SOURCES                                │
│  OpenAQ S3 Archive ──── Batch (CSV.gz) ────┐                    │
│  OpenAQ API v3     ── Streaming (JSON) ──┐ │                    │
└──────────────────────────────────────────┼─┼────────────────────┘
                                           │ │
                      ┌────────────────────┘ │
                      │    ┌─────────────────┘
                      ▼    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION                                   │
│                                                                 │
│  Batch:     raw_locations.py ─── OpenAQ API → BigQuery          │
│             raw_measurements.py ─ S3 CSV.gz → BigQuery          │
│                                                                 │
│  Streaming: producer.py ─── OpenAQ API → Redpanda               │
│             consumer.py ─── Redpanda → BigQuery                 │
└────────────────────────────────┬────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│               TRANSFORMATION (Bruin SQL Assets)                 │
│                                                                 │
│  air_quality_raw          air_quality_staging     air_quality_marts
│  ├── locations            ├── measurements        ├── fct_city_daily_aqi
│  ├── measurements         │   (clean, normalize,  └── dim_stations
│  └── measurements_stream  │    deduplicate)
│                           │
│  Quality checks: 17 passing (not_null, unique, non_negative,    │
│                   accepted_values, positive)                    │
└────────────────────────────────┬────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION                               │
│  Bruin CLI — built-in DAG resolution from `depends:` metadata   │
│  Pipeline schedule: daily                                       │
│  Assets execute in order: locations → measurements → staging    │
│                           → fct_city_daily_aqi + dim_stations   │
└────────────────────────────────┬────────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DASHBOARD                                  │
│  Looker Studio (connected to BigQuery marts)                    │
│  Tiles: KPI scorecards, bubble map, bar chart, time series      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Infrastructure as Code | Terraform | Provision GCS bucket + 3 BigQuery datasets |
| Containerization | Docker Compose | Run Redpanda (Kafka-compatible broker) + Console |
| Data Ingestion (batch) | Python + Bruin | Download CSV.gz from OpenAQ S3, load to BigQuery |
| Data Ingestion (stream) | Kafka (Redpanda) | Producer polls OpenAQ API, consumer writes to BigQuery |
| Data Warehouse | Google BigQuery | Partitioned by date, clustered by parameter |
| Transformations | Bruin SQL assets | Staging (clean + normalize) → Marts (analytics) |
| Data Quality | Bruin built-in checks | 17 checks: not_null, unique, non_negative, accepted_values |
| Orchestration | Bruin CLI | DAG resolution, scheduling, dependency management |
| Dashboard | Looker Studio | 4-tile interactive dashboard |

---

## Cloud & Infrastructure as Code

The project runs entirely on **Google Cloud Platform**. All cloud resources are provisioned and managed with **Terraform**:

**Terraform-managed resources (`infrastructure/terraform/main.tf`):**

- **GCS Bucket** (`dtc-de-course-454704-air-quality-raw`): Raw data landing zone with lifecycle rules (auto-transition to Nearline after 90 days) and versioning enabled.
- **BigQuery Dataset** `air_quality_raw`: Raw ingested data from batch and streaming sources.
- **BigQuery Dataset** `air_quality_staging`: Cleaned, deduplicated, unit-normalized data.
- **BigQuery Dataset** `air_quality_marts`: Analytics-ready tables for the dashboard.

Existing resources were imported into Terraform state using `terraform import`, ensuring the IaC reflects the actual cloud environment.

---

## Data Ingestion

### Batch Ingestion

Two Python assets handle batch ingestion, orchestrated by Bruin's dependency system:

**Asset 1: `air_quality_raw.locations`** (Python)
- Fetches all 300 Mexico monitoring station metadata from the OpenAQ v3 API.
- Loads station ID, name, locality, coordinates, sensor list, and sensor count into BigQuery.
- Quality checks: `not_null` on location_id and name, `unique` on location_id.

**Asset 2: `air_quality_raw.measurements`** (Python)
- Reads the list of Mexico location IDs from BigQuery (depends on Asset 1).
- For each location, downloads daily CSV.gz files from the OpenAQ S3 archive (`s3://openaq-data-archive/records/csv.gz/locationid={id}/year={year}/month={month}/`).
- Parses the 9-column CSV schema (location_id, sensors_id, location, datetime, lat, lon, parameter, units, value).
- Loads to BigQuery with `WRITE_APPEND` disposition.
- Quality checks: `not_null` on location_id, parameter, and value.

### Streaming Ingestion

**Producer** (`producer.py`):
- Polls the OpenAQ v3 API endpoint `/v3/locations/{id}/latest` for each Mexico station.
- Publishes measurement records as JSON messages to the `openaq-measurements` Redpanda topic (3 partitions).
- Message key: `{location_id}_{parameter}` for partition affinity.
- Configurable poll interval and location count.

**Consumer** (`consumer.py`):
- Consumes from the `openaq-measurements` topic.
- Micro-batches records and flushes to BigQuery (`air_quality_raw.measurements_stream`) every N seconds or M records.
- The target table is partitioned by `datetime_utc` (DAY) and clustered by `parameter`.
- Handles backpressure via configurable batch size and flush interval.

**Infrastructure**: Redpanda runs via Docker Compose with a web console for debugging.

---

## Data Warehouse

BigQuery is organized in three layers:

### `air_quality_raw` (Raw Layer)

| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `locations` | OpenAQ API | 300 | Station metadata for Mexico |
| `measurements` | S3 batch | 58,238+ | Historical sensor readings |
| `measurements_stream` | Kafka consumer | Ongoing | Real-time sensor readings |

### `air_quality_staging` (Staging Layer)

| Table | Description |
|-------|-------------|
| `measurements` | Cleaned, deduplicated, unit-normalized readings from both batch and stream sources |

**Optimizations applied:**
- **Deduplication**: `ROW_NUMBER()` window function partitioned by `(location_id, parameter, datetime)` keeps only the latest ingestion per reading.
- **Unit normalization**: Converts ppm → µg/m³ for all gaseous pollutants using standard conversion factors (CO: ×1145, NO2: ×1880, SO2: ×2620, O3: ×1960, NO: ×1230, NOx: ×1880).
- **Outlier filtering**: Removes negative values (sensor errors) and extreme outliers (value ≥ 10,000).
- **Time dimension extraction**: Pre-computes `measurement_date`, `measurement_hour`, and `day_of_week` for efficient downstream queries.

### `air_quality_marts` (Marts Layer)

**`fct_city_daily_aqi`** — City-level daily air quality metrics:
- Grain: one row per city × date × parameter.
- Aggregates: city average, min, max, station count, reading count, coverage percentage.
- WHO AQI categories for PM2.5 and PM10 (Good / Moderate / Unhealthy for Sensitive Groups / Unhealthy / Very Unhealthy / Hazardous).
- Boolean flag `exceeds_who_guideline` for PM2.5, PM10, NO2, and O3.
- Includes city centroid coordinates for map visualization.

**`dim_stations`** — Station dimension with reliability scoring:
- Grain: one row per station.
- Activity metrics: first/last reading date, active days, total readings.
- Reliability tier (High/Medium/Low) based on uptime percentage.
- Parameters observed and sensor count.

---

## Transformations

Transformations are implemented as **Bruin SQL assets** with built-in quality checks declared in the asset metadata. This is Bruin's equivalent of dbt models + tests in a single file.

**Staging asset** (`stg_measurements.sql`):
- Unions batch and stream raw tables.
- Normalizes units to µg/m³.
- Deduplicates by (location_id, parameter, datetime).
- Extracts time dimensions.
- Applies data quality filters.
- **Quality checks**: not_null (location_id, parameter, value_normalized, measurement_datetime), non_negative (value_normalized).

**Mart assets** (`fct_city_daily_aqi.sql`, `dim_stations.sql`):
- Aggregate from staging to city-day and station grains.
- Apply WHO AQI classification logic.
- Calculate reliability metrics.
- **Quality checks**: not_null, positive (station_count), unique (location_id in dim), accepted_values (reliability_tier).

The Bruin DAG executes assets in dependency order:
```
raw_locations → raw_measurements → stg_measurements → fct_city_daily_aqi
                                                    → dim_stations
```

---

## Dashboard

The Looker Studio dashboard connects directly to the BigQuery marts and provides 4 interactive tiles:

| Tile | Type | Data Source | Description |
|------|------|-------------|-------------|
| KPI Scorecards | Scorecards | Both marts | Stations (151), Cities (91), WHO Exceedance Rate (41.18%), Date range selector |
| Pollution Map | Google Maps (Bubble) | fct_city_daily_aqi | Bubble map of Mexico showing PM2.5 hotspots, colored by intensity (green → red) |
| Worst Cities | Horizontal bar chart | fct_city_daily_aqi | Top 10 most polluted cities by average PM2.5 |
| Daily Trends | Time series | fct_city_daily_aqi | PM2.5 daily trend lines for the top cities |

All tiles are filtered to PM2.5 by default and respond to the date range control.

---

## Project Structure

```
air-quality-bruin/
├── .bruin.yml                                    # Project config + GCP connection
├── .gitignore
├── README.md
│
├── infrastructure/
│   ├── terraform/
│   │   ├── main.tf                               # GCS bucket + 3 BigQuery datasets
│   │   ├── outputs.tf                            # Output values
│   │   ├── terraform.tfvars                      # Variables (git-ignored)
│   │   └── .terraform.lock.hcl                   # Provider lock
│   └── docker/
│       └── docker-compose.yml                    # Redpanda + Console
│
├── pipelines/air_quality/
│   ├── pipeline.yml                              # Pipeline definition + daily schedule
│   └── assets/
│       ├── ingest/
│       │   ├── raw_locations.py                   # OpenAQ API → BigQuery (300 stations)
│       │   ├── raw_measurements.py                # S3 CSV.gz → BigQuery (58K+ rows)
│       │   └── requirements.txt                   # Python dependencies
│       ├── staging/
│       │   └── stg_measurements.sql               # Clean + normalize + deduplicate
│       ├── marts/
│       │   ├── fct_city_daily_aqi.sql             # City AQI with WHO categories
│       │   └── dim_stations.sql                   # Station reliability dimension
│       └── streaming/
│           ├── producer.py                        # OpenAQ API → Redpanda
│           ├── consumer.py                        # Redpanda → BigQuery
│           └── requirements.txt                   # Python dependencies
```

---

## Reproducibility

### Prerequisites

- **Google Cloud Platform** account with billing enabled
- **Terraform** ≥ 1.0
- **Docker** & Docker Compose
- **Python** 3.10+
- **Bruin CLI** ([install guide](https://bruin-data.github.io/bruin/getting-started/introduction/installation.html))
- **AWS CLI** (for S3 access, no credentials needed — public bucket)
- **OpenAQ API key** (free, register at [explore.openaq.org/register](https://explore.openaq.org/register))

### Step 1: Clone and Configure

```bash
git clone <repository-url>
cd air-quality-bruin
```

Create `infrastructure/terraform/terraform.tfvars`:
```hcl
project_id       = "your-gcp-project-id"
credentials_file = "/path/to/your/service-account-key.json"
```

Update `.bruin.yml` with your GCP connection:
```yaml
default_environment: default
environments:
    default:
        connections:
            google_cloud_platform:
                - name: gcp
                  project_id: "your-gcp-project-id"
                  service_account_file: "/path/to/your/service-account-key.json"
```

Update the Python assets (`raw_locations.py`, `raw_measurements.py`, `consumer.py`) with your:
- `BQ_PROJECT` (GCP project ID)
- `OPENAQ_API_KEY` (your OpenAQ API key)
- `SA_FILE` in consumer.py (path to service account key)

### Step 2: Provision Infrastructure

```bash
cd infrastructure/terraform
terraform init
terraform apply
```

This creates the GCS bucket and 3 BigQuery datasets.

### Step 3: Run the Batch Pipeline

```bash
cd ../..
git init  # Bruin requires a git repo
bruin validate .
bruin run pipelines/air_quality
```

This runs all 5 assets in dependency order with quality checks. Expected output:
```
bruin run completed successfully
 ✓ Assets executed      5 succeeded
 ✓ Quality checks       17 succeeded
```

### Step 4: Run Streaming (Optional)

Start Redpanda:
```bash
cd infrastructure/docker
docker compose up -d
docker exec redpanda rpk topic create openaq-measurements --partitions 3
```

In terminal 1 — start the producer:
```bash
cd pipelines/air_quality/assets/streaming
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python producer.py --max-locations 20 --interval 300
```

In terminal 2 — start the consumer:
```bash
cd pipelines/air_quality/assets/streaming
source .venv/bin/activate
python consumer.py --flush-interval 15
```

### Step 5: Dashboard

The live dashboard is available at: [https://lookerstudio.google.com/s/pHmo9v6Jo-o](https://lookerstudio.google.com/s/pHmo9v6Jo-o)

To build your own:

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Create a new Blank Report
3. Add data source: BigQuery → your project → `air_quality_marts` → `fct_city_daily_aqi`
4. Add second data source: `air_quality_marts` → `dim_stations`
5. Build the 4 tiles as described in the Dashboard section above 