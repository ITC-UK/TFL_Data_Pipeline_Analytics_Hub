# -*- coding: utf-8 -*-
import time
import requests
import json
import os
from kafka import KafkaProducer
import yaml

with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]
POLL_INTERVAL = cfg["tfl"]["polling_interval"]

TFL_APP_ID = cfg["tfl"]["app_id"]
TFL_APP_KEY = cfg["tfl"]["app_key"]
API_LIST = cfg["tfl"]["api_list"]

def create_producer(servers):
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def fetch_tfl_data(api_url, app_id, app_key):
    """Fetch JSON data from TFL API."""
    url = "{}?app_id={}&app_key={}".format(api_url, app_id, app_key)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def enrich_events(events, line_name):
    """Add line metadata to each event."""
    for event in events:
        event["line"] = line_name
    return events

def send_events(producer, topic, events):
    """Send events to Kafka."""
    for event in events:
        producer.send(topic, value=event)
    producer.flush()

def poll_and_send(producer, topic, api_list, app_id, app_key):
    """Fetch data from all APIs and send to Kafka."""
    for line_name, api in api_list.items():
        events = fetch_tfl_data(api, app_id, app_key)
        enriched = enrich_events(events, line_name)
        send_events(producer, topic, enriched)
        print("[SENT] {} updates".format(line_name))

def main(poll_interval=30):
    """Main loop to continuously poll TFL and send updates to Kafka."""
    producer = create_producer(KAFKA_SERVER)
    while True:
        print("Pulling TFL updates...")
        poll_and_send(producer, TOPIC, API_LIST, TFL_APP_ID, TFL_APP_KEY)
        print("Sleeping {} seconds...\n".format(poll_interval))
        time.sleep(poll_interval)

if __name__ == "__main__":
    main()