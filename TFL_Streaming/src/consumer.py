# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark session
spark = (
    SparkSession.builder
    .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
    .getOrCreate()
)

# Kafka config
kafka_servers = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
topic = "ukde011025tfldata"

# HDFS paths
incoming_path = "hdfs:///tmp/DE011025/uk/streaming/incoming/"
checkpoint_path = "hdfs:///tmp/DE011025/uk/streaming/incoming_checkpoints/"

# TFL API Schema
tfl_schema = ArrayType(StructType([
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

# Read stream from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .load()
)

raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

# Parse JSON safely with from_json
parsed = raw_json_df.withColumn("data", from_json(col("json_value"), tfl_schema))


def process_batch(batch_df, batch_id):
    # Filter out rows where JSON parsing failed
    valid_rows_df = batch_df.filter(col("data").isNotNull())

    # Explode the array to get individual events
    exploded = valid_rows_df.select(explode(col("data")).alias("event"))
    df = exploded.select("event.*")

    # 👉 Print schema and sample data here
    print("\n=== SCHEMA FOR BATCH {} ===".format(batch_id))
    df.printSchema()

    print("\n=== SAMPLE ROWS FOR BATCH {} ===".format(batch_id))
    df.show(5, truncate=False)

    # Write in append mode
    df.write.mode("append").parquet(incoming_path)

    print("Wrote batch {} to incoming parquet".format(batch_id))



# Apply foreachBatch for proper streaming writes
query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .start()
)

query.awaitTermination()
