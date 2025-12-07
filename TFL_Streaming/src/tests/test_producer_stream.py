import pytest
from unittest.mock import MagicMock, patch
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ----------------------------
# Patch config BEFORE import
# ----------------------------
with patch("builtins.open", MagicMock()), \
     patch("yaml.safe_load", return_value={
         "kafka": {"bootstrap_servers": ["localhost:9092"], "topic": "topic"},
         "tfl": {"app_id": "APP", "app_key": "KEY", "api_list": {"line": "http://api"}},
         "hdfs": {"checkpoint_path": "/tmp/checkpoint", "incoming_path": "/tmp/incoming"}
     }):
    import producer  # noqa: E402

# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

@pytest.fixture
def sample_events():
    """Return a sample list of TFL events."""
    return [{"id": "1", "destinationName": "Station"}]

# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

def test_create_producer_returns_kafka_producer(monkeypatch):
    """Ensure create_producer returns a KafkaProducer instance with correct serializers."""
    mock_kafka = MagicMock()
    monkeypatch.setattr(producer, "KafkaProducer", mock_kafka)

    kafka = producer.create_producer(["localhost:9092"])
    assert kafka == mock_kafka.return_value

    vs = mock_kafka.call_args.kwargs["value_serializer"]
    ks = mock_kafka.call_args.kwargs["key_serializer"]
    assert vs({"a": 1}) == b'{"a": 1}'
    assert ks("key") == b"key"


def test_fetch_tfl_data_returns_json():
    """Verify fetch_tfl_data calls session.get and returns parsed JSON."""
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ok": True}
    mock_session.get.return_value = mock_resp

    result = producer.fetch_tfl_data(mock_session, "http://api", "APP", "KEY")
    assert result == {"ok": True}
    mock_session.get.assert_called_once()
    mock_resp.raise_for_status.assert_called_once()


def test_enrich_events_adds_line_metadata():
    """Ensure enrich_events adds line metadata and converts IDs to strings."""
    events = [{"id": 5}, {"id": "7"}]
    enriched = producer.enrich_events(events, "central")

    for event in enriched:
        assert event["line"] == "central"
        assert isinstance(event["id"], str)


def test_send_events_calls_producer_methods():
    """Verify send_events calls producer.send for each event and flushes."""
    mock_producer = MagicMock()
    events = [{"id": "1"}, {"id": "2"}]

    sent_count = producer.send_events(mock_producer, "topic", events)
    assert sent_count == 2
    assert mock_producer.send.call_count == 2
    mock_producer.flush.assert_called_once()


@patch("producer.fetch_tfl_data")
@patch("producer.send_events")
def test_poll_and_send_once_invokes_pipeline(mock_send, mock_fetch):
    """Ensure poll_and_send_once fetches, enriches, and sends events via Kafka."""
    mock_fetch.return_value = [{"id": "1"}]
    mock_send.return_value = 1
    mock_producer = MagicMock()
    api_list = {"victoria": "http://api/vic"}

    total = producer.poll_and_send_once(mock_producer, api_list, "APP", "KEY", MagicMock())

    mock_fetch.assert_called_once()
    mock_send.assert_called_once()
    enriched_events = mock_send.call_args[0][2]
    assert enriched_events[0]["line"] == "victoria"
    assert total == 1


def test_shutdown_closes_producer():
    """Verify shutdown flushes and closes the Kafka producer."""
    mock_prod = MagicMock()
    producer.shutdown(mock_prod)
    mock_prod.flush.assert_called_once()
    mock_prod.close.assert_called_once()
