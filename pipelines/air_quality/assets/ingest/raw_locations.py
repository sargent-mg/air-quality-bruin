"""@bruin
name: air_quality_raw.locations
type: python
depends: []

secrets:
  - key: gcp
    inject_as: GCP_CREDENTIALS

columns:
  - name: location_id
    type: integer
    checks:
      - name: not_null
      - name: unique
  - name: name
    type: varchar
    checks:
      - name: not_null
@bruin"""

import os
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

OPENAQ_API_KEY = os.environ.get("OPENAQ_API_KEY")
COUNTRIES_ID = 157  # Mexico
BQ_PROJECT = os.environ.get("GCP_PROJECT_ID", "dtc-de-course-454704")
BQ_DATASET = "air_quality_raw"
BQ_TABLE = "locations"


def get_bq_client():
    """Create BigQuery client using Bruin-injected credentials."""
    creds_json = os.environ.get("GCP_CREDENTIALS")
    if creds_json:
        creds_dict = json.loads(creds_json)
        # Bruin injects the connection as JSON with project_id and service_account_file
        sa_file = creds_dict.get("service_account_file")
        if sa_file:
            credentials = service_account.Credentials.from_service_account_file(sa_file)
            return bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    # Fallback to default credentials
    return bigquery.Client(project=BQ_PROJECT)


def fetch_locations():
    """Fetch all Mexico locations from OpenAQ API."""
    all_locations = []
    page = 1

    while True:
        response = requests.get(
            "https://api.openaq.org/v3/locations",
            headers={"X-API-Key": OPENAQ_API_KEY},
            params={"countries_id": COUNTRIES_ID, "page": page, "limit": 1000},
        )
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])

        if not results:
            break

        for loc in results:
            sensors = loc.get("sensors", [])
            all_locations.append({
                "location_id": loc.get("id"),
                "name": loc.get("name"),
                "locality": loc.get("locality"),
                "timezone": loc.get("timezone"),
                "is_mobile": loc.get("isMobile", False),
                "is_monitor": loc.get("isMonitor", False),
                "latitude": loc.get("coordinates", {}).get("latitude") if loc.get("coordinates") else None,
                "longitude": loc.get("coordinates", {}).get("longitude") if loc.get("coordinates") else None,
                "parameters": json.dumps([s["parameter"]["name"] for s in sensors]),
                "sensor_count": len(sensors),
            })

        if len(results) < 1000:
            break
        page += 1

    return all_locations


def load_to_bigquery(rows):
    """Load location records into BigQuery."""
    client = get_bq_client()
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("location_id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("locality", "STRING"),
            bigquery.SchemaField("timezone", "STRING"),
            bigquery.SchemaField("is_mobile", "BOOLEAN"),
            bigquery.SchemaField("is_monitor", "BOOLEAN"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("parameters", "STRING"),
            bigquery.SchemaField("sensor_count", "INTEGER"),
        ],
    )

    job = client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {len(rows)} locations into {table_ref}")


def main():
    print("Fetching Mexico locations from OpenAQ API...")
    locations = fetch_locations()
    print(f"Found {len(locations)} locations.")

    print("Loading into BigQuery...")
    load_to_bigquery(locations)
    print("Done.")


if __name__ == "__main__":
    main()