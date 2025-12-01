import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import Row

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../TFL_Streaming/src')))
import consumer

from consumer import (
    get_tfl_schema,
    parse_json,
    filter_valid,
    explode_events,
    write_output,
    process_batch,
)

# Create a Spark session once per test session
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("unit-tests") \
        .getOrCreate()


def test_get_tfl_schema_returns_array_of_struct():
    # Explanation:
    # Ensures the schema returned matches the expected top-level ArrayType.
    schema = get_tfl_schema()
    assert schema.typeName() == "array"
    assert schema.elementType.typeName() == "struct"
    assert len(schema.elementType.fields) == 19


def test_parse_json_parses_valid_json(spark):
    # Explanation:
    # Verifies that valid JSON is parsed into a "data" column following the schema.
    schema = get_tfl_schema()
    json_df = spark.createDataFrame(
        [('{ "id": "1", "operationType": 1 }',)],
        ["json_value"]
    )
    parsed = parse_json(json_df, schema)
    assert "data" in parsed.columns
    row = parsed.collect()[0]
    assert row.data is not None


def test_filter_valid_removes_null_data(spark):
    # Explanation:
    # Ensures rows where "data" is null are dropped.
    df = spark.createDataFrame(
        [
            Row(data=None),
            Row(data={"id": "1"})
        ]
    )
    filtered = filter_valid(df)
    assert filtered.count() == 1
    assert filtered.collect()[0].data["id"] == "1"


def test_explode_events_expands_array_to_rows(spark):
    # Explanation:
    # Confirms explode_events takes an array column and outputs one row per element.
    df = spark.createDataFrame(
        [
            Row(data=[{"id": "1"}, {"id": "2"}])
        ]
    )
    exploded = explode_events(df)
    ids = [row.id for row in exploded.collect()]
    assert ids == ["1", "2"]


def test_write_output_writes_parquet(monkeypatch, spark, tmp_path):
    # Explanation:
    # Ensures write_output triggers a parquet write by intercepting the Spark write call.
    captured = {}

    class MockWriter:
        def mode(self, mode):
            return self
        def parquet(self, path):
            captured["path"] = path

    def fake_writer(self):
        return MockWriter()

    df = spark.createDataFrame([Row(id="1")])
    monkeypatch.setattr(df, "write", fake_writer(df))

    write_output(df, str(tmp_path))
    assert captured["path"] == str(tmp_path)


def test_process_batch_executes_pipeline(monkeypatch, spark, tmp_path):
    # Explanation:
    # Verifies process_batch calls filter, explode, and write in sequence by mocking write_output.
    calls = {"write_called": False}

    def mock_write(df, path):
        calls["write_called"] = True
        calls["path"] = path
        calls["df_count"] = df.count()

    monkeypatch.setattr("consumer.write_output", mock_write)

    df = spark.createDataFrame([
        Row(json_value='[{ "id": "1" }]')
    ])

    # Prepare schema and parse JSON first because process_batch expects parsed structure
    parsed = parse_json(df, get_tfl_schema())

    process_batch(parsed, batch_id=1, output_path=str(tmp_path))

    assert calls["write_called"] is True
    assert calls["df_count"] == 1
    assert calls["path"] == str(tmp_path)
