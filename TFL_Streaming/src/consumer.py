# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_tfl_schema():
    """Return the Spark schema for TFL API data, with timestamps as STRING."""
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
        StructField("timestamp", StringType()),           # Keep as string initially
        StructField("timeToStation", IntegerType()),
        StructField("currentLocation", StringType()),
        StructField("towards", StringType()),
        StructField("expectedArrival", StringType()),     # Keep as string initially
        StructField("timeToLive", StringType()),          # Keep as string initially
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

def convert_timestamps(df):
    """Convert timestamp strings to TimestampType columns."""
    return df.withColumn("timestamp", to_timestamp("timestamp")) \
             .withColumn("expectedArrival", to_timestamp("expectedArrival")) \
             .withColumn("timeToLive", to_timestamp("timeToLive"))

def write_output(df, path):
    """Write DataFrame to Parquet in append mode."""
    df.write.mode("append").parquet(path)

def process_batch(batch_df, batch_id, output_path):
    """Inside foreachBatch, just write the batch as-is."""
    batch_df.printSchema()
    batch_df.show(5, truncate=False)
    write_output(batch_df, output_path)


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
    valid_df = filter_valid(parsed_df)
    exploded_df = explode_events(valid_df)
    converted_df = convert_timestamps(exploded_df)

    # Apply watermark and deduplicate BEFORE foreachBatch
    deduped_streaming_df = converted_df \
        .withWatermark("timestamp", "10 minutes") \
        .dropDuplicates(["id", "stationName", "timestamp"])

    def batch_fn(df, batch_id):
        process_batch(df, batch_id, incoming_path)

    query = (
        deduped_streaming_df.writeStream
        .foreachBatch(batch_fn)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
