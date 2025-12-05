# ==========================================================================================
# PRODUCTION Spark Structured Streaming Consumer
# Overwrite-mode snapshot consumer (keeps only the latest API data)
# ==========================================================================================

import os
import logging
from datetime import datetime
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, to_timestamp,
    current_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import *

# --- logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tfl-consumer")

# --- load config ---
with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]

CHECKPOINT_PATH = cfg["hdfs"]["checkpoint_path"]
INCOMING_PATH = cfg["hdfs"]["incoming_path"]
PREFERRED_SINK = cfg.get("sink", {}).get("preferred", "delta")
WATERMARK_DELAY = cfg.get("streaming", {}).get("watermark_delay", "2 minutes")
BATCH_PROCESS_LIMIT = cfg.get("streaming", {}).get("batch_limit", None)

# --- Spark session ---
spark = (
    SparkSession.builder
    .appName("UK_TFL_STREAMING_CONSUMER_SNAPSHOT")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# Kafka source
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Schema
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
        StructField("timestamp", StringType()),
        StructField("timeToStation", IntegerType()),
        StructField("currentLocation", StringType()),
        StructField("towards", StringType()),
        StructField("expectedArrival", StringType()),
        StructField("timeToLive", StringType()),
        StructField("modeName", StringType()),
    ]))

# Parsing logic
def parse_and_flatten(df):
    schema = get_tfl_schema()
    parsed = df.withColumn("data", from_json(col("json_value"), schema))
    valid = parsed.filter(col("data").isNotNull())
    exploded = valid.select(explode(col("data")).alias("event"))

    flat = exploded.select("event.*")
    flat = flat.withColumn("timestamp", to_timestamp(col("timestamp")))
    flat = flat.withColumn("expectedArrival", to_timestamp(col("expectedArrival")))
    flat = flat.withColumn("timeToLive", to_timestamp(col("timeToLive")))

    flat = flat.withColumn("_ingest_ts", current_timestamp())
    flat = flat.withColumn("year", year(col("_ingest_ts")))
    flat = flat.withColumn("month", month(col("_ingest_ts")))
    flat = flat.withColumn("day", dayofmonth(col("_ingest_ts")))

    return flat

# ------------------------------------------------------------------------------------------
# NEW OVERWRITE IMPLEMENTATIONS
# ------------------------------------------------------------------------------------------

def write_using_delta_overwrite(df, target_path):
    """Overwrite entire Delta table with latest snapshot."""
    from delta.tables import DeltaTable
    try:
        logger.info(f"Overwriting Delta table at {target_path}")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        logger.info("Delta overwrite SUCCESS.")
    except Exception as e:
        logger.error(f"Delta overwrite FAILED: {e}")
        raise

def write_using_parquet_overwrite(df, target_path):
    """Overwrite entire Parquet folder with latest snapshot."""
    try:
        logger.info(f"Overwriting Parquet directory at {target_path}")
        df.write.mode("overwrite").parquet(target_path)
        logger.info("Parquet overwrite SUCCESS.")
    except Exception as e:
        logger.error(f"Parquet overwrite FAILED: {e}")
        raise

# ------------------------------------------------------------------------------------------
# foreachBatch logic
# ------------------------------------------------------------------------------------------

def foreach_batch_function(batch_df, batch_id):
    logger.info(f"Processing batch {batch_id} - count={batch_df.count()}")

    if batch_df.rdd.isEmpty():
        logger.info("Empty batch; skipping.")
        return

    flat = parse_and_flatten(batch_df)

    candidate = (
        flat
        .withWatermark("expectedArrival", WATERMARK_DELAY)
        .dropDuplicates(["id"])   # STILL deduping!
    )

    if BATCH_PROCESS_LIMIT:
        candidate = candidate.limit(int(BATCH_PROCESS_LIMIT))

    # Preferred sink: Delta overwrite
    if PREFERRED_SINK.lower() == "delta":
        try:
            write_using_delta_overwrite(candidate, INCOMING_PATH)
            return
        except Exception:
            logger.exception("Delta overwrite failed; falling back to Parquet.")

    # Parquet overwrite fallback
    write_using_parquet_overwrite(candidate, INCOMING_PATH)

    logger.info(f"Finished processing batch {batch_id}")

# ------------------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------------------

# # Commented version for continuous streaming with trigger(processingTime)
# def main():
#     raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")
#     query = (
#         raw_json_df.writeStream
#         .foreachBatch(foreach_batch_function)
#         .option("checkpointLocation", CHECKPOINT_PATH)
#         .trigger(processingTime="30 seconds")  # tune as required
#         .start()
#     )
#     logger.info(f"Started streaming query with checkpoint={CHECKPOINT_PATH}")
#     query.awaitTermination()

def main():
    raw_json_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_value")

    query = (
        raw_json_df.writeStream
        .foreachBatch(foreach_batch_function)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(once=True)
        .start()
    )

    logger.info(f"Streaming job started. Checkpoint={CHECKPOINT_PATH}")
    query.awaitTermination()

if __name__ == "__main__":
    main()
