"""
Kafka Producer: Polls OpenAQ API for latest Mexico air quality
measurements and publishes them to Redpanda.

Usage:
  python producer.py
  python producer.py --interval 300 --max-locations 50
"""

import json
import time
import argparse
from datetime import datetime, timezone
from confluent_kafka import Producer
import requests

OPENAQ_API_KEY = "5d93a44bd5e4316c4a423647efd98464eab25f759f3420d9446f93195ce77f34"
OPENAQ_API_URL = "https://api.openaq.org/v3"
COUNTRIES_ID = 157  # Mexico

KAFKA_TOPIC = "openaq-measurements"
KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"


def delivery_report(err, msg):
    if err is not None:
        print(f"  Delivery failed: {err}")


def fetch_mexico_location_ids():
    """Fetch all Mexico location IDs."""
    ids = []
    page = 1
    while True:
        response = requests.get(
            f"{OPENAQ_API_URL}/locations",
            headers={"X-API-Key": OPENAQ_API_KEY},
            params={"countries_id": COUNTRIES_ID, "limit": 1000, "page": page},
            timeout=30,
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            break
        ids.extend([loc["id"] for loc in results])
        if len(results) < 1000:
            break
        page += 1
    return ids


def fetch_latest_for_location(location_id):
    """Fetch latest measurements for a single location."""
    try:
        response = requests.get(
            f"{OPENAQ_API_URL}/locations/{location_id}/latest",
            headers={"X-API-Key": OPENAQ_API_KEY},
            params={"limit": 100},
            timeout=15,
        )
        if response.status_code == 404:
            return []
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"  API error for location {location_id}: {e}")
        return []


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "openaq-producer",
        "acks": "all",
    })


def publish_measurements(producer, location_id, measurements):
    published = 0
    for record in measurements:
        try:
            param = record.get("parameter", {})
            param_name = param.get("name", "unknown") if isinstance(param, dict) else str(param)

            key = f"{location_id}_{param_name}"
            value = json.dumps({
                "location_id": location_id,
                **record,
                "produced_at": datetime.now(timezone.utc).isoformat(),
            }, default=str)

            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_report,
            )
            published += 1
        except Exception as e:
            print(f"  Failed to produce: {e}")

    producer.flush()
    return published


def run_producer(interval_seconds, max_locations):
    producer = create_producer()

    print("Fetching Mexico location IDs...")
    location_ids = fetch_mexico_location_ids()
    if max_locations:
        location_ids = location_ids[:max_locations]
    print(f"Tracking {len(location_ids)} locations")
    print(f"Polling every {interval_seconds}s")

    cycle = 0
    while True:
        cycle += 1
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n[{timestamp}] Cycle {cycle}: Fetching latest measurements...")

        total_published = 0
        for loc_id in location_ids:
            measurements = fetch_latest_for_location(loc_id)
            if measurements:
                count = publish_measurements(producer, loc_id, measurements)
                total_published += count

            # Small delay to avoid API rate limits
            time.sleep(0.2)

        print(f"[{timestamp}] Published {total_published} measurements from {len(location_ids)} locations")
        print(f"Sleeping {interval_seconds}s...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OpenAQ → Kafka Producer")
    parser.add_argument("--interval", type=int, default=300, help="Poll interval in seconds")
    parser.add_argument("--max-locations", type=int, default=20, help="Max locations to poll (for testing)")
    args = parser.parse_args()

    run_producer(args.interval, args.max_locations)