"""
Kafka Consumer: Reads air quality measurements from Redpanda
and writes them to BigQuery in micro-batches.

Usage:
  python consumer.py
  python consumer.py --batch-size 200 --flush-interval 30
"""

import json
import time
import argparse
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery
from google.oauth2 import service_account

KAFKA_TOPIC = "openaq-measurements"
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
KAFKA_GROUP_ID = "openaq-bq-consumer"

BQ_PROJECT = os.environ.get("GCP_PROJECT_ID", "dtc-de-course-454704")
BQ_DATASET = "air_quality_raw"
BQ_TABLE = "measurements_stream"
SA_FILE = os.environ.get("GCP_SA_FILE")


def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(SA_FILE)
    return bigquery.Client(project=BQ_PROJECT, credentials=credentials)


def ensure_table(client):
    """Create the stream table if it doesn't exist."""
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    schema = [
        bigquery.SchemaField("location_id", "INTEGER"),
        bigquery.SchemaField("sensors_id", "INTEGER"),
        bigquery.SchemaField("value", "FLOAT64"),
        bigquery.SchemaField("datetime_utc", "TIMESTAMP"),
        bigquery.SchemaField("datetime_local", "STRING"),
        bigquery.SchemaField("latitude", "FLOAT64"),
        bigquery.SchemaField("longitude", "FLOAT64"),
        bigquery.SchemaField("produced_at", "TIMESTAMP"),
        bigquery.SchemaField("consumed_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_ref, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        field="datetime_utc",
        type_=bigquery.TimePartitioningType.DAY,
    )

    try:
        client.create_table(table)
        print(f"Created table {table_ref}")
    except Exception:
        print(f"Table {table_ref} already exists")


def parse_measurement(raw):
    """Parse a Kafka message into a BigQuery row."""
    try:
        dt = raw.get("datetime", {})
        coords = raw.get("coordinates", {})

        return {
            "location_id": raw.get("location_id") or raw.get("locationsId"),
            "sensors_id": raw.get("sensorsId"),
            "value": raw.get("value"),
            "datetime_utc": dt.get("utc") if isinstance(dt, dict) else dt,
            "datetime_local": dt.get("local") if isinstance(dt, dict) else None,
            "latitude": coords.get("latitude") if isinstance(coords, dict) else None,
            "longitude": coords.get("longitude") if isinstance(coords, dict) else None,
            "produced_at": raw.get("produced_at"),
            "consumed_at": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        print(f"  Parse error: {e}")
        return None


def write_batch(client, rows):
    if not rows:
        return 0
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        print(f"  BQ insert errors: {errors[:3]}")
        return len(rows) - len(errors)
    return len(rows)


def run_consumer(batch_size, flush_interval):
    bq_client = get_bq_client()
    ensure_table(bq_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    buffer = []
    last_flush = time.time()
    total_written = 0

    print(f"Consumer started. Topic: {KAFKA_TOPIC}")
    print(f"Batch size: {batch_size}, Flush interval: {flush_interval}s")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"  Kafka error: {msg.error()}")
                continue
            else:
                try:
                    raw = json.loads(msg.value().decode("utf-8"))
                    parsed = parse_measurement(raw)
                    if parsed:
                        buffer.append(parsed)
                except json.JSONDecodeError as e:
                    print(f"  Bad message: {e}")

            elapsed = time.time() - last_flush
            if len(buffer) >= batch_size or (buffer and elapsed >= flush_interval):
                timestamp = datetime.now().strftime("%H:%M:%S")
                written = write_batch(bq_client, buffer)
                total_written += written
                print(f"[{timestamp}] Flushed {written} rows (total: {total_written})")
                consumer.commit()
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print(f"\nShutting down. Total written: {total_written}")
    finally:
        if buffer:
            write_batch(bq_client, buffer)
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka → BigQuery Consumer")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--flush-interval", type=int, default=15)
    args = parser.parse_args()

    run_consumer(args.batch_size, args.flush_interval)