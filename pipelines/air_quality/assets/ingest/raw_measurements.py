"""@bruin
name: air_quality_raw.measurements
type: python
depends:
  - air_quality_raw.locations

secrets:
  - key: gcp
    inject_as: GCP_CREDENTIALS

columns:
  - name: location_id
    type: integer
    checks:
      - name: not_null
  - name: parameter
    type: varchar
    checks:
      - name: not_null
  - name: value
    type: float
    checks:
      - name: not_null
@bruin"""

import os
import io
import csv
import gzip
import json
from datetime import datetime, timedelta

import requests as http_requests
from google.cloud import bigquery
from google.oauth2 import service_account

BQ_PROJECT = "dtc-de-course-454704"
BQ_DATASET = "air_quality_raw"
S3_BASE = "https://openaq-data-archive.s3.amazonaws.com/records/csv.gz"

# How many days back to load (adjust for backfills)
LOOKBACK_DAYS = 7


def get_bq_client():
    """Create BigQuery client using Bruin-injected credentials."""
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if creds_json:
        creds_dict = json.loads(creds_json)
        sa_file = creds_dict.get("service_account_file")
        if sa_file:
            credentials = service_account.Credentials.from_service_account_file(sa_file)
            return bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    return bigquery.Client(project=BQ_PROJECT)


def get_mexico_location_ids(client):
    """Fetch all Mexico location IDs from BigQuery."""
    query = f"SELECT location_id FROM `{BQ_PROJECT}.{BQ_DATASET}.locations`"
    result = client.query(query).result()
    return [row.location_id for row in result]


def download_and_parse(location_id, year, month, day):
    """Download a single CSV.gz file from S3 and return parsed rows."""
    date_str = f"{year}{month:02d}{day:02d}"
    url = f"{S3_BASE}/locationid={location_id}/year={year}/month={month:02d}/location-{location_id}-{date_str}.csv.gz"

    try:
        resp = http_requests.get(url, timeout=15)
        if resp.status_code == 404:
            return []
        if resp.status_code == 403:
            return []
        resp.raise_for_status()

        content = gzip.decompress(resp.content)
        reader = csv.DictReader(io.StringIO(content.decode("utf-8")))

        rows = []
        for row in reader:
            try:
                rows.append({
                    "location_id": int(row["location_id"]),
                    "sensors_id": int(row["sensors_id"]),
                    "location": row["location"],
                    "datetime": row["datetime"],
                    "lat": float(row["lat"]) if row["lat"] else None,
                    "lon": float(row["lon"]) if row["lon"] else None,
                    "parameter": row["parameter"],
                    "units": row["units"],
                    "value": float(row["value"]) if row["value"] else None,
                })
            except (ValueError, KeyError) as e:
                continue
        return rows
    except Exception as e:
        return []


def load_to_bigquery(client, rows):
    """Load measurement rows into BigQuery."""
    if not rows:
        return 0

    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.measurements"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("location_id", "INTEGER"),
            bigquery.SchemaField("sensors_id", "INTEGER"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("datetime", "TIMESTAMP"),
            bigquery.SchemaField("lat", "FLOAT64"),
            bigquery.SchemaField("lon", "FLOAT64"),
            bigquery.SchemaField("parameter", "STRING"),
            bigquery.SchemaField("units", "STRING"),
            bigquery.SchemaField("value", "FLOAT64"),
        ],
    )

    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    return len(rows)


def main():
    client = get_bq_client()

    # Get Mexico location IDs
    location_ids = get_mexico_location_ids(client)
    print(f"Found {len(location_ids)} location IDs to process.")

    # Generate date range
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)

    total_rows = 0
    total_files = 0

    for loc_id in location_ids:
        loc_rows = []
        current = start_date

        while current <= end_date:
            rows = download_and_parse(
                loc_id, current.year, current.month, current.day
            )
            if rows:
                loc_rows.extend(rows)
                total_files += 1
            current += timedelta(days=1)

        # Load per location to avoid memory issues
        if loc_rows:
            loaded = load_to_bigquery(client, loc_rows)
            total_rows += loaded
            print(f"  Location {loc_id}: {loaded} rows from {total_files} files")

    print(f"\nDone. Total: {total_rows} rows from {total_files} files.")


if __name__ == "__main__":
    main()