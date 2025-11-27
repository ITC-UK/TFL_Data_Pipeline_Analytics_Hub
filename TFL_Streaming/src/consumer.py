from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------------------------------------------------------------
# 1. CREATE SPARK SESSION
# ------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
        .getOrCreate()
)

kafka_servers = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
topic = "ukde011025tfldata"

# ------------------------------------------------------------
# 2. OFFICIAL TFL ARRIVALS API SCHEMA
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 3. READ STREAM FROM KAFKA (REAL-TIME)
# ------------------------------------------------------------
kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
)

raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

# ------------------------------------------------------------
# 4. PARSE JSON USING from_json() SAFELY (STREAMING-SAFE)
# ------------------------------------------------------------
parsed = raw_json_df.withColumn(
    "data", from_json(col("json_value"), tfl_schema)
).select(explode(col("data")).alias("event"))

# Flatten fields
df = parsed.select("event.*")

# ------------------------------------------------------------
# 5. CREATE EVENT TIMESTAMP & UNIQUE ROW KEY
# ------------------------------------------------------------
df = df.withColumn("event_ts", to_timestamp(col("timestamp")))

df = df.withColumn(
    "unique_key",
    concat_ws(
        "_",
        col("id"),
        col("naptanId"),
        col("expectedArrival")
    )
)

# ------------------------------------------------------------
# 6. DEDUPLICATION WITH WATERMARK
# ------------------------------------------------------------
deduped = (
    df
        .withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["unique_key"])
)

# ------------------------------------------------------------
# 7. WRITE TO HDFS (PARQUET, NO PARTITIONS)
# ------------------------------------------------------------
query = (
    deduped.writeStream
        .format("parquet")
        .option("path", "hdfs:////tmp/DE011025/uk/streaming/incoming_batches/")
        .option("checkpointLocation", "hdfs:////tmp/DE011025/uk/streaming/incoming_batches/checkpoints/")
        .outputMode("append")
        .start()
)

query.awaitTermination()
