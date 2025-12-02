import pytest
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import producer


@pytest.fixture
def sample_events():
    """Return a list of sample TFL events."""
    return [{"id": "1", "destinationName": "Station"}]


def test_create_producer_returns_kafka_producer(monkeypatch):
    """Ensure create_producer returns a KafkaProducer instance with JSON serialization."""
    mock_kafka_class = MagicMock()
    monkeypatch.setattr(producer, "KafkaProducer", mock_kafka_class)

    kafka = producer.create_producer(["localhost:9092"])
    assert kafka == mock_kafka_class.return_value
    # Check serializer converts dict to JSON string bytes
    serialized = kafka.value_serializer({"key": "value"})
    assert serialized == b'{"key": "value"}'


@patch("producer.requests.get")
def test_fetch_tfl_data_returns_json(mock_get):
    """Verify fetch_tfl_data calls requests.get and returns parsed JSON."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data": 1}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    result = producer.fetch_tfl_data("http://api.test", "id", "key")
    assert result == {"data": 1}
    mock_get.assert_called_once_with("http://api.test")


def test_enrich_events_adds_line_metadata(sample_events):
    """Ensure enrich_events adds the line name to each event."""
    enriched = producer.enrich_events(sample_events, "piccadilly")
    for event in enriched:
        assert event["line"] == "piccadilly"


def test_send_events_calls_producer_methods(sample_events):
    """Verify send_events calls producer.send for each event and flushes."""
    mock_producer = MagicMock()
    producer.send_events(mock_producer, "topic", sample_events)
    assert mock_producer.send.call_count == len(sample_events)
    mock_producer.flush.assert_called_once()


@patch("producer.fetch_tfl_data")
@patch("producer.send_events")
def test_poll_and_send_calls_fetch_and_send(mock_send, mock_fetch, sample_events):
    """Ensure poll_and_send fetches data, enriches it, and sends via Kafka."""
    mock_fetch.return_value = sample_events
    mock_producer = MagicMock()
    api_list = {"testline": "http://api.test"}

    producer.poll_and_send(mock_producer, "topic", api_list, "id", "key")

    mock_fetch.assert_called_once_with("http://api.test", "id", "key")
    mock_send.assert_called_once()
    sent_events = mock_send.call_args[0][2]  # Extract events sent
    assert all("line" in e for e in sent_events)
    assert sent_events[0]["line"] == "testline"
