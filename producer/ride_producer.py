import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from google.cloud import pubsub_v1

PROJECT_ID = "ride-hailing-analytics-512806"
TOPIC_ID = "rides-events"
TOTAL_EVENTS = 1000
DURATION_SECONDS = 600

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

zones = ["Manhattan", "Brooklyn", "Queens", "Bronx", "JFK"]

start_time = datetime.now(timezone.utc)
start_monotonic = time.monotonic()
futures = []

for i in range(TOTAL_EVENTS):
    elapsed = time.monotonic() - start_monotonic
    target_elapsed = i * DURATION_SECONDS / TOTAL_EVENTS
    if target_elapsed > elapsed:
        time.sleep(target_elapsed - elapsed)

    pickup = start_time + timedelta(seconds=target_elapsed)
    duration_minutes = random.randint(5, 90)
    dropoff = pickup + timedelta(minutes=duration_minutes)

    event = {
        "ride_id": str(uuid.uuid4()),
        "driver_id": f"driver_{random.randint(1, 100)}",
        "rider_id": f"rider_{random.randint(1, 500)}",
        "pickup_zone": random.choice(zones),
        "dropoff_zone": random.choice(zones),
        "pickup_ts": pickup.isoformat(),
        "dropoff_ts": dropoff.isoformat(),
        "ingest_ts": datetime.now(timezone.utc).isoformat(),
        "fare": round(random.uniform(5, 100), 2),
        "surge_multiplier": round(random.choice([1.0, 1.0, 1.0, 1.25, 1.5, 2.0]), 2),
    }

    futures.append(publisher.publish(
        topic_path,
        json.dumps(event).encode("utf-8"),
        event_type="ride",
    ))

for future in futures:
    future.result()

publisher.transport.close()
print(f"Published {len(futures)} ride events to {topic_path}")
