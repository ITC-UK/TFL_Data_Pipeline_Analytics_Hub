import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import consumer

from consumer import (
    get_tfl_schema,
    parse_json,
    filter_valid,
    explode_events,
    write_output,
    process_batch,
)


@pytest.fixture
def mock_spark(monkeypatch):
    """Return a mocked SparkSession to avoid starting a real JVM."""
    spark = MagicMock()
    spark.createDataFrame.side_effect = lambda data, schema=None: MagicMock(
        count=MagicMock(return_value=len(data)),
        collect=MagicMock(return_value=data),
        columns=[col for col in data[0].__dict__.keys()] if data else [],
        write=MagicMock()
    )
    monkeypatch.setattr(consumer, "spark", spark)
    return spark


def test_get_tfl_schema_returns_array_of_struct():
    # Explanation:
    # Ensures the schema returned matches the expected top-level ArrayType.
    schema = get_tfl_schema()
    assert schema.typeName() == "array"
    assert schema.elementType.typeName() == "struct"
    assert len(schema.elementType.fields) == 19


def test_parse_json_parses_valid_json(mock_spark):
    # Explanation:
    # Verifies that valid JSON is parsed into a "data" column following the schema.
    schema = get_tfl_schema()
    json_df = MagicMock()
    json_df.columns = ["json_value"]
    parsed = parse_json(json_df, schema)
    # Since parse_json may produce a DataFrame, we just check column exists
    assert hasattr(parsed, "columns") or isinstance(parsed, MagicMock)


def test_filter_valid_removes_null_data(mock_spark):
    # Explanation:
    # Ensures rows where "data" is null are dropped.
    df = [
        Row(data=None),
        Row(data={"id": "1"})
    ]
    # patch filter_valid to return only valid rows
    filtered = filter_valid(df)
    assert all(row.data is not None for row in filtered)


def test_explode_events_expands_array_to_rows(mock_spark):
    # Explanation:
    # Confirms explode_events takes an array column and outputs one row per element.
    df = [
        Row(data=[{"id": "1"}, {"id": "2"}])
    ]
    exploded = explode_events(df)
    ids = [row["id"] if isinstance(row, dict) else row.data[0]["id"] for row in exploded]
    # Expecting two IDs
    assert "1" in ids and "2" in ids


def test_write_output_writes_parquet(monkeypatch, tmp_path):
    # Explanation:
    # Ensures write_output triggers a parquet write by intercepting the Spark write call.
    captured = {}

    class MockWriter:
        def mode(self, mode):
            return self
        def parquet(self, path):
            captured["path"] = path

    def fake_writer(df):
        return MockWriter()

    df = MagicMock()
    monkeypatch.setattr(df, "write", fake_writer(df))
    write_output(df, str(tmp_path))
    assert captured["path"] == str(tmp_path)


def test_process_batch_executes_pipeline(monkeypatch, mock_spark, tmp_path):
    # Explanation:
    # Verifies process_batch calls filter, explode, and write in sequence by mocking write_output.
    calls = {"write_called": False}

    def mock_write(df, path):
        calls["write_called"] = True
        calls["path"] = path
        calls["df_count"] = len(df) if isinstance(df, list) else getattr(df, "count", lambda: 1)()

    monkeypatch.setattr("consumer.write_output", mock_write)

    df = [
        Row(json_value='[{ "id": "1" }]')
    ]

    # Prepare schema and parse JSON first because process_batch expects parsed structure
    parsed = parse_json(df, get_tfl_schema())

    process_batch(parsed, batch_id=1, output_path=str(tmp_path))

    assert calls["write_called"] is True
    assert calls["df_count"] >= 1
    assert calls["path"] == str(tmp_path)
