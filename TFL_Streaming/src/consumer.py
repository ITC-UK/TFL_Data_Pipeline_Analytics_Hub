from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
 
spark = SparkSession.builder \
    .appName("UK_TFL_DE011025_CONSUMER") \
    .getOrCreate()
 
kafka_bootstrap = "ip-172-31-3-80.eu-west-2.compute.internal:9092"
topic = "ukde011025tfldata"
 
# Read messages once (non-streaming)
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()
 
# Convert value to string
parsed_df = df.selectExpr("CAST(value AS STRING) as json_value")

# Print to stout for Jeinkins logging
parsed_df.show(truncate=False)

# Save to HDFS
parsed_df.write.mode("overwrite").json("hdfs:////tmp/DE011025/uk/streaming/incoming_batches")