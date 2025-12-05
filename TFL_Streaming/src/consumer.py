"""
Production-ready Spark Structured Streaming consumer with deduplication.

Features:
- Uses foreachBatch to do robust batch processing
- Preferred sink: Delta Lake (MERGE on id) if available
- Fallback sink: Parquet partition-level upsert with dedupe
- Uses event 'id' + watermarking for safe dropDuplicates
- Partitioned by date (year/month/day) for faster reads
- Checkpointing is still configured via writeStream.option('checkpointLocation', ...)
"""

import os
import logging
from datetime import datetime
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, to_timestamp, lit, current_timestamp, year, month, dayofmonth
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
INCOMING_PATH = cfg["hdfs"]["incoming_path"]  # base path for output
PREFERRED_SINK = cfg.get("sink", {}).get("preferred", "delta")  # "delta" or "parquet"
WATERMARK_DELAY = cfg.get("streaming", {}).get("watermark_delay", "2 minutes")  # adjust as needed
BATCH_PROCESS_LIMIT = cfg.get("streaming", {}).get("batch_limit", None)  # optional

# --- Spark session ---
spark = (
    SparkSession.builder
    .appName("UK_TFL_STREAMING_CONSUMER_DEDUPED")
    # if you have Delta, ensure the jars are available in your cluster (Databricks or delta-core)
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# Kafka source DF
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

def get_tfl_schema():
    # match TFL fields (timestamp fields as string -> cast later)
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
        StructField("timestamp", StringType()),         # parse to timestamp below
        StructField("timeToStation", IntegerType()),
        StructField("currentLocation", StringType()),
        StructField("towards", StringType()),
        StructField("expectedArrival", StringType()),   # parse to timestamp below
        StructField("timeToLive", StringType()),
        StructField("modeName", StringType())
    ]))

def parse_and_flatten(df):
    """Parse json string and explode array into rows, cast timestamps, add ingestion_ts."""
    schema = get_tfl_schema()
    parsed = df.withColumn("data", from_json(col("json_value"), schema))
    valid = parsed.filter(col("data").isNotNull())
    exploded = valid.select(explode(col("data")).alias("event"))
    # flatten
    flat = exploded.select("event.*")
    # cast timestamp-like strings into proper timestamps (TFL often uses ISO strings)
    flat = flat.withColumn("timestamp", to_timestamp(col("timestamp")))
    flat = flat.withColumn("expectedArrival", to_timestamp(col("expectedArrival")))
    flat = flat.withColumn("timeToLive", to_timestamp(col("timeToLive")))
    # add ingestion time for record keeping
    flat = flat.withColumn("_ingest_ts", current_timestamp())
    # compute partitioning columns (date)
    flat = flat.withColumn("year", year(col("_ingest_ts"))).withColumn("month", month(col("_ingest_ts"))).withColumn("day", dayofmonth(col("_ingest_ts")))
    return flat

def write_using_delta(df, target_path):
    """Merge into Delta table by id. Requires delta to be installed."""
    from delta.tables import DeltaTable  # will raise if not present
    delta_path = target_path.rstrip("/")  # delta table path
    try:
        if not DeltaTable.isDeltaTable(spark, delta_path):
            logger.info(f"Creating new Delta table at {delta_path}")
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path)
            logger.info(f"Delta table created successfully at {delta_path}")
            return

        delta_table = DeltaTable.forPath(spark, delta_path)
        logger.info(f"Merging batch into existing Delta table at {delta_path}")
        merge_cond = "target.id = updates.id"
        delta_table.alias("target").merge(
            df.alias("updates"),
            merge_cond
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info(f"Delta merge completed successfully to {delta_path}")
    except Exception as e:
        logger.error(f"Delta write failed at {delta_path}: {e}", exc_info=True)
        raise

def write_using_parquet_upsert(df, target_base_path):
    """
    Fallback upsert:
    - Partition by year/month/day (based on _ingest_ts)
    - For each partition in batch, read existing partition files (if present),
      union with incoming, dropDuplicates on 'id', and overwrite that partition.
    NOTE: This can be I/O heavy for large historic partitions.
    """
    # collect partitions present in this batch
    parts = df.select("year", "month", "day").distinct().collect()
    partitions = [(int(r.year), int(r.month), int(r.day)) for r in parts]
    logger.info(f"Partitions to upsert: {partitions}")
    for y, m, d in partitions:
        part_path = os.path.join(target_base_path, f"year={y}/month={m}/day={d}")
        logger.info(f"Writing partition for {y}-{m}-{d} to {part_path}")
        incoming_part = df.filter((col("year") == y) & (col("month") == m) & (col("day") == d))

        # Check if partition exists
        existing = None
        try:
            # Try catalog table check first
            if spark._jsparkSession.catalog().tableExists(part_path):
                existing = spark.read.parquet(part_path)
            else:
                # fallback to filesystem path check
                from py4j.java_gateway import java_import
                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
                path = spark._jvm.org.apache.hadoop.fs.Path(part_path)
                if fs.exists(path):
                    existing = spark.read.parquet(part_path)
        except Exception as e:
            logger.warning(f"Error checking existing partition at {part_path}: {e}")

        try:
            if existing is None:
                incoming_part.write.mode("append").parquet(part_path)
                logger.info(f"Wrote new partition {part_path} successfully")
            else:
                combined = existing.unionByName(incoming_part)
                deduped = combined.dropDuplicates(["id"])
                tmp = part_path + "_tmp_" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
                deduped.write.mode("overwrite").parquet(tmp)
                # move/replace partition atomically
                hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
                dst = spark._jvm.org.apache.hadoop.fs.Path(part_path)
                src = spark._jvm.org.apache.hadoop.fs.Path(tmp)
                if hadoop_fs.exists(dst):
                    hadoop_fs.delete(dst, True)
                hadoop_fs.rename(src, dst)
                logger.info(f"Upserted partition {part_path} successfully")
        except Exception as e:
            logger.error(f"Failed writing partition {part_path}: {e}", exc_info=True)
            raise


def foreach_batch_function(batch_df, batch_id):
    logger.info(f"Starting processing batch {batch_id} with {batch_df.count()} rows")
    if batch_df.rdd.isEmpty():
        logger.info("Empty batch, skipping")
        return

    # batch_df already has 'json_value' as STRING, no need to cast again
    flat = parse_and_flatten(batch_df)

    # watermark + dedupe on event id and event time
    time_col = "expectedArrival"
    candidate = flat.withWatermark(time_col, WATERMARK_DELAY).dropDuplicates(["id"])

    if BATCH_PROCESS_LIMIT:
        candidate = candidate.limit(int(BATCH_PROCESS_LIMIT))

    if PREFERRED_SINK.lower() == "delta":
        try:
            logger.info(f"Attempting Delta upsert to {INCOMING_PATH}")
            write_using_delta(candidate, INCOMING_PATH)
            logger.info(f"Batch {batch_id} written successfully using Delta")
            return
        except Exception:
            logger.exception("Delta upsert failed, falling back to parquet upsert")

    # fallback parquet write
    try:
        logger.info(f"Attempting parquet upsert to {INCOMING_PATH}")
        write_using_parquet_upsert(candidate, INCOMING_PATH)
        logger.info(f"Batch {batch_id} written successfully using Parquet")
    except Exception:
        logger.exception("Parquet upsert failed")

    logger.info(f"Finished processing batch {batch_id}")


# Commented version for continuous streaming with trigger(processingTime)
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
        # trigger once for batch processing and immediate completion
        .trigger(once=True)
        .start()
    )
    logger.info(f"Started streaming query with checkpoint={CHECKPOINT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
