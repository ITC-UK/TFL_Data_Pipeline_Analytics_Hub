# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

# Create Spark session with Deequ jar
spark = (
    SparkSession.builder
    .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
    .config("spark.jars.packages", "com.amazon.deequ:deequ:1.2.2-spark-3.3")
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
parsed = raw_json_df.withColumn(
    "data", from_json(col("json_value"), tfl_schema)
)


def run_schema_validation(batch_df, batch_id):
    print("Running Deequ validation on batch {}".format(batch_id))

    valid_rows_df = batch_df.filter(col("data").isNotNull())

    if valid_rows_df.count() == 0:
        raise Exception("JSON parsing failed. Incoming data does not match schema.")

    exploded = valid_rows_df.select(explode(col("data")).alias("event"))
    df = exploded.select("event.*")

    check = (Check(spark, CheckLevel.Error, "schemacheck")
             .isComplete("id")
             .isComplete("naptanId")
             .isComplete("stationName")
             .isComplete("lineId")
             .isComplete("expectedArrival")
             .isInt("operationType")
             .isInt("timeToStation")
             .isContainedIn("modeName", ["tube"])
             )

    results = VerificationSuite(spark).onData(df).addCheck(check).run()
    status = results.checkResults["schemacheck"].status

    print("Deequ results: {}".format(status))

    if status != "Success":
        raise Exception("Schema validation failed in batch {} - data does not match schema.".format(batch_id))

    print("Batch schema validated successfully")

def write_to_incoming(batch_df, batch_id):
    exploded = batch_df.select(explode(col("data")).alias("event"))
    df = exploded.select("event.*")

    # Append mode for streaming safe writes
    df.write.mode("append").parquet(incoming_path)
    print("Wrote validated batch {} to incoming parquet".format(batch_id))


# Apply validation and write for each micro-batch
query = (
    parsed.writeStream
    .foreachBatch(lambda batch_df, batch_id: (
        run_schema_validation(batch_df, batch_id),
        write_to_incoming(batch_df, batch_id)
    ))
    .option("checkpointLocation", checkpoint_path)
    .start()
)

query.awaitTermination()
