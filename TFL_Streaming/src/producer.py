# -*- coding: utf-8 -*-
import time
import requests
import json
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
import yaml

<<<<<<< Updated upstream
API_LIST = {
    "piccadilly":   "https://api.tfl.gov.uk/Line/Piccadilly/Arrivals",
    "northern":     "https://api.tfl.gov.uk/Line/Northern/Arrivals",
    "central":      "https://api.tfl.gov.uk/Line/Central/Arrivals",
    "bakerloo":     "https://api.tfl.gov.uk/Line/Bakerloo/Arrivals",
    "metropolitan": "https://api.tfl.gov.uk/Line/Metropolitan/Arrivals",
    "victoria":     "https://api.tfl.gov.uk/Line/Victoria/Arrivals"
}

APP_ID  = "92293faa428041caad3dd647d39753a0"
APP_KEY = "ba72936a3db54b4ba5792dc8f7acc043"
TOPIC   = "ukde011025tfldata"
KAFKA_SERVERS = ['ip-172-31-3-80.eu-west-2.compute.internal:9092']

def create_producer(servers):
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_tfl_data(api_url, app_id, app_key):
    """Fetch JSON data from TFL API."""
    url = f"{api_url}?app_id={app_id}&app_key={app_key}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def enrich_events(events, line_name):
    """Add line metadata to each event."""
    for event in events:
        event["line"] = line_name
    return events
=======
# Load .env
load_dotenv()

# Load config
with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]
POLL_INTERVAL = cfg["tfl"]["polling_interval"]

TFL_APP_ID = os.getenv("TFL_APP_ID")
TFL_APP_KEY = os.getenv("TFL_APP_KEY")
API_LIST = cfg["tfl"]["api_list"]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    print("Pulling TFL updates...")
    for line_name, api_url in API_LIST.items():
        url = f"{api_url}?app_id={TFL_APP_ID}&app_key={TFL_APP_KEY}"
        data = requests.get(url).json()

        for event in data:
            event["line"] = line_name

            producer.send(TOPIC, value=event)

        print(f"[SENT] {line_name} updates")
>>>>>>> Stashed changes

def send_events(producer, topic, events):
    """Send events to Kafka."""
    for event in events:
        producer.send(topic, value=event)
    producer.flush()
<<<<<<< Updated upstream

def poll_and_send(producer, topic, api_list, app_id, app_key):
    """Fetch data from all APIs and send to Kafka."""
    for line_name, api in api_list.items():
        events = fetch_tfl_data(api, app_id, app_key)
        enriched = enrich_events(events, line_name)
        send_events(producer, topic, enriched)
        print(f"[SENT] {line_name} updates")

def main(poll_interval=30):
    """Main loop to continuously poll TFL and send updates to Kafka."""
    producer = create_producer(KAFKA_SERVERS)
    while True:
        print("Pulling TFL updates...")
        poll_and_send(producer, TOPIC, API_LIST, APP_ID, APP_KEY)
        print(f"Sleeping {poll_interval} seconds...\n")
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()
=======
    print(f"Sleeping {POLL_INTERVAL} seconds...\n")
    time.sleep(POLL_INTERVAL)
>>>>>>> Stashed changes
