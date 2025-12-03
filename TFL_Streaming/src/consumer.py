import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import *
import yaml

with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]
POLL_INTERVAL = cfg["tfl"]["polling_interval"]

TFL_APP_ID = os.getenv("TFL_APP_ID")
TFL_APP_KEY = os.getenv("TFL_APP_KEY")
API_LIST = cfg["tfl"]["api_list"]
CHECKPOINT_PATH = cfg["hdfs"]["checkpoint_path"]
INCOMING_PATH = cfg["hdfs"]["incoming_path"]

spark = (
    SparkSession.builder
    .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
    .getOrCreate())

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load())

def get_tfl_schema():
    return ArrayType(StructType([
        StructField("id", StringType()),
        StructField("operationType", IntegerType()),
        StructField("vehicleId", StringType()),
        StructField("naptanId", StringType()),
        StructField("stationName", StringType()),
        StructField("lineId", StringType()),
        StructField("lineName", StringType()),
        StructField("platformName", StringType()),
        StructField("direction", StringType()),
        StructField("bearing", StringType()),
        StructField("destinationNaptanId", StringType()),
        StructField("destinationName", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("timeToStation", IntegerType()),
        StructField("currentLocation", StringType()),
        StructField("towards", StringType()),
        StructField("expectedArrival", TimestampType()),
        StructField("timeToLive", TimestampType()),
        StructField("modeName", StringType())
    ]))

def parse_json(df, schema):
    """Parse JSON column 'json_value' into structured DataFrame using schema."""
    return df.withColumn("data", from_json(col("json_value"), schema))

def filter_valid(df):
    """Filter out rows where JSON parsing failed (null data)."""
    return df.filter(col("data").isNotNull())

def explode_events(df):
    """Explode array of events into individual rows."""
    exploded = df.select(explode(col("data")).alias("event"))
    return exploded.select("event.*")

def write_output(df, path):
    """Write DataFrame to Parquet in append mode."""
    df.write.mode("append").parquet(path)


def process_batch(batch_df, batch_id, output_path):
    """Process each micro-batch: validate, explode, show, and write."""
    valid = filter_valid(batch_df)
    events_df = explode_events(valid)
    events_df.printSchema()
    events_df.show(5, truncate=False)
    write_output(events_df, output_path)


def batch_fn(df, batch_id):
    process_batch(df, batch_id, INCOMING_PATH)


def main():
    """Run Spark streaming consumer for TFL Kafka topic."""
    raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
    schema = get_tfl_schema()
    parsed_df = parse_json(raw_json_df, schema)

    def batch_fn(df, batch_id):
        process_batch(df, batch_id, INCOMING_PATH)

    query = (
        parsed_df.writeStream
        .foreachBatch(batch_fn)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()