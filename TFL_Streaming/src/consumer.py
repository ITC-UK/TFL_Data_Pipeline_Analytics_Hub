import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import *
from dotenv import load_dotenv
import yaml

<<<<<<< Updated upstream
def get_tfl_schema():
    """Return the Spark schema for TFL API data."""
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
        StructField("timestamp", StringType()),
        StructField("timeToStation", IntegerType()),
        StructField("currentLocation", StringType()),
        StructField("towards", StringType()),
        StructField("expectedArrival", StringType()),
        StructField("timeToLive", StringType()),
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

def main():
    """Run Spark streaming consumer for TFL Kafka topic."""
    kafka_servers = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
    topic = "ukde011025tfldata"
    incoming_path = "hdfs:///tmp/DE011025/uk/streaming/incoming/"
    checkpoint_path = "hdfs:///tmp/DE011025/uk/streaming/incoming_checkpoints/"

    spark = (
        SparkSession.builder
        .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
    schema = get_tfl_schema()
    parsed_df = parse_json(raw_json_df, schema)

    def batch_fn(df, batch_id):
        process_batch(df, batch_id, incoming_path)

    query = (
        parsed_df.writeStream
        .foreachBatch(batch_fn)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()
=======
# Load environment & config
load_dotenv()
with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]

INCOMING_PATH = cfg["hdfs"]["incoming_path"]
CHECKPOINT_PATH = cfg["hdfs"]["checkpoint_path"]

# Load schema
with open("config/schema/tfl_stream_schema.json") as f:
    schema_json = json.load(f)

tfl_schema = ArrayType(StructType([
    StructField(field["name"], StringType() if field["type"]=="string" else IntegerType())
    for field in schema_json
]))

spark = SparkSession.builder.appName("UK_TFL_STREAMING_CONSUMER_DEDUPED").getOrCreate()

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
parsed = raw_json_df.withColumn("data", from_json(col("json_value"), tfl_schema))

def process_batch(batch_df, batch_id):
    valid_rows_df = batch_df.filter(col("data").isNotNull())
    exploded = valid_rows_df.select(explode(col("data")).alias("event"))
    df = exploded.select("event.*")
    df.write.mode("append").parquet(INCOMING_PATH)
    print(f"Wrote batch {batch_id} to incoming parquet")

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

query.awaitTermination()
>>>>>>> Stashed changes
