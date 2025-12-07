# -*- coding: utf-8 -*-
"""
Single-run TFL polling producer (Jenkins-friendly).
- Fetches once from all TFL APIs
- Sends results to Kafka
- Idempotent Kafka delivery with acks=all, retries
- Robust requests.Session with retries
- Clean shutdown handling
"""

import json
import logging
import signal
import sys
from typing import Dict, List
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from kafka import KafkaProducer
import yaml

# --- logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]  # Ensure logs go to stdout for Jenkins console
)
logger = logging.getLogger("tfl-producer")

# --- load config ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "dev.yaml")

try:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    logger.info("Loaded config from config/dev.yaml")
except Exception as e:
    logger.exception("Failed to load config file")
    sys.exit(1)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]

TFL_APP_ID = cfg["tfl"]["app_id"]
TFL_APP_KEY = cfg["tfl"]["app_key"]
API_LIST = cfg["tfl"]["api_list"]

RUNNING = True


def create_requests_session(total_retries=5, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    """HTTP session with retries/backoff."""
    s = requests.Session()

    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        # allowed_methods=frozenset(["GET", "POST"])  # removed for compatibility
    )

    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    logger.info("Created requests session with retries")
    return s


def create_producer(servers):
    """Kafka producer with idempotence + acks=all."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            retries=5,
            linger_ms=50,
            max_in_flight_requests_per_connection=1,
            # enable_idempotence=True,  # removed for compatibility
        )
        logger.info(f"KafkaProducer created connected to {servers}")
        return producer
    except Exception as e:
        logger.exception(f"Failed to create KafkaProducer: {e}")
        sys.exit(1)


def fetch_tfl_data(session: requests.Session, api_url: str, app_id: str, app_key: str, timeout=10):
    """Fetch JSON from TFL API."""
    url = f"{api_url}?app_id={app_id}&app_key={app_key}"
    logger.info(f"Fetching data from {url}")
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    logger.info(f"Received {len(data)} records from {url}")
    return data


def enrich_events(events: List[Dict], line_name: str) -> List[Dict]:
    """Add line metadata + ensure id is string."""
    output = []
    for event in events or []:
        if not isinstance(event, dict):
            continue

        event = dict(event)
        event.setdefault("line", line_name)

        if "id" in event:
            event["id"] = str(event["id"])

        output.append(event)
    logger.info(f"Enriched {len(output)} events for line {line_name}")
    return output


def send_events(producer: KafkaProducer, topic: str, events: List[Dict]):
    """Send events to Kafka using event['id'] as key."""
    sent = 0
    for event in events:
        try:
            key = event.get("id") or f"{event.get('line','unknown')}-{event.get('naptanId','')}-{event.get('timestamp','')}"
            producer.send(topic, key=key, value=event)
            sent += 1
        except Exception:
            logger.exception("Failed sending event to Kafka")

    try:
        producer.flush(timeout=10)
        logger.info("Kafka producer flush completed")
    except Exception:
        logger.exception("Flush failed")

    logger.info("Sent %d events to Kafka topic %s", sent, topic)
    return sent


def poll_and_send_once(producer: KafkaProducer, api_list: Dict, app_id: str, app_key: str, session: requests.Session):
    """Fetch once from all TFL APIs and send to Kafka."""
    total = 0
    for line_name, api in api_list.items():
        try:
            raw = fetch_tfl_data(session, api, app_id, app_key)
            enriched = enrich_events(raw, line_name)
            total += send_events(producer, TOPIC, enriched)
            logger.info("[SENT] %s updates: %d", line_name, len(enriched))
        except Exception:
            logger.exception("Failed fetching/sending for %s", line_name)
    logger.info(f"Total events sent this run: {total}")
    return total


def shutdown(producer: KafkaProducer = None):
    """Graceful shutdown."""
    global RUNNING
    RUNNING = False
    logger.info("Shutting down producer...")
    if producer:
        try:
            producer.flush(timeout=10)
            producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception:
            logger.exception("Error closing producer")
    logger.info("Producer stopped.")


def _signal_handler(sig, frame):
    logger.info("Received signal %s, shutting down...", sig)
    shutdown()


def main():
    """Single-run producer entrypoint."""
    session = create_requests_session()
    producer = create_producer(KAFKA_SERVER)

    # handle CTRL+C or Jenkins stop
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info("Running single-run TFL producer...")

    try:
        poll_and_send_once(producer, API_LIST, TFL_APP_ID, TFL_APP_KEY, session)
    except Exception:
        logger.exception("Producer run failed with an exception")
        sys.exit(1)
    finally:
        shutdown(producer)


if __name__ == "__main__":
    main()
