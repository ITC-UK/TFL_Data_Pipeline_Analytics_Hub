# -*- coding: utf-8 -*-
"""
TFL SILVER TRANSFORMATION
Reads ALL bronze run folders (full + incremental)
Handles schema differences automatically
Works with part, part-*, and any file format inside run_*.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, lit,
    regexp_replace, to_timestamp
)
from functools import reduce

BRONZE_BASE = "hdfs:///tmp/DE011025/TFL_Batch_processing/bronze"

line_groups = [
    "bakerloo",
    "central",
    "metropolitan",
    "northern",
    "piccadilly",
    "victoria"
]

silver_path = "hdfs:///tmp/DE011025/TFL_Batch_processing/tfl_silver_incremental"

SILVER_COLS = [
    "id","vehicleid","naptanid","stationname","lineid","linename","line_group",
    "platformname","direction","destinationnaptanid","destinationname",
    "event_time","timetostation","currentlocation","towards",
    "expectedarrival_ts","train_type"
]

def align_and_union(df1, df2):
    """Union two DataFrames with different schemas"""
    columns = list(set(df1.columns) | set(df2.columns))
    for c in columns:
        if c not in df1.columns:
            df1 = df1.withColumn(c, lit(None))
        if c not in df2.columns:
            df2 = df2.withColumn(c, lit(None))
    return df1.select(sorted(columns)).union(df2.select(sorted(columns)))

spark = (
    SparkSession.builder
    .appName("TFL_Silver_Transformation")
    .enableHiveSupport()
    .getOrCreate()
)

cleaned_dfs = []

for line_group in line_groups:
    # Load ALL run_* folders, ANY part files inside
    path = f"{BRONZE_BASE}/TFL_{line_group}_lines/run_*/*"
    print("Reading:", path)

    df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")       # << Auto-detect schema differences
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )

    # Rename columns automatically (Spark may generate col_0, col_1…)
    # Read the first line of data to determine original CSV header length
    # Your Bronze files ALWAYS have 30 columns so enforce consistent naming:
    raw_cols = [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","direction","bearing","destinationnaptanid",
        "destinationname","timestamp_str","timetostation","currentlocation","towards",
        "expectedarrival","timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received","api_fetch_time"
    ]

    for i, colname in enumerate(df.columns):
        df = df.withColumnRenamed(colname, raw_cols[i])

    # CLEAN STRING FIELDS
    df = (
        df.withColumn("stationname", trim(col("stationname")))
          .withColumn("linename", trim(col("linename")))
          .withColumn("platformname", trim(col("platformname")))
          .withColumn("direction", trim(col("direction")))
          .withColumn("destinationname", trim(col("destinationname")))
          .withColumn("currentlocation", trim(col("currentlocation")))
          .withColumn("towards", trim(col("towards")))
    )

    # TIMESTAMP FIXING
    df = df.withColumn(
        "event_time",
        when(col("timestamp_str").rlike("\\.\\d+Z$"),
            to_timestamp(regexp_replace(col("timestamp_str"), "Z$", ""),
                         "yyyy-MM-dd'T'HH:mm:ss.SSS")
        ).otherwise(
            to_timestamp(regexp_replace(col("timestamp_str"), "Z$", ""),
                         "yyyy-MM-dd'T'HH:mm:ss")
        )
    )

    df = df.withColumn(
        "expectedarrival_ts",
        to_timestamp(regexp_replace(col("expectedarrival"), "Z$", ""),
                     "yyyy-MM-dd'T'HH:mm:ss")
    )

    # SAFE CAST
    df = df.withColumn(
        "timetostation",
        when(trim(col("timetostation").cast("string")).rlike("^[0-9]+$"),
             col("timetostation").cast("int"))
        .otherwise(lit(None))
    )

    # DIRECTION FIX
    df = df.withColumn(
        "direction",
        when(
            col("direction").isNull() | (col("direction") == ""),
            when(lower(col("platformname")).contains("east"), "eastbound")
            .when(lower(col("platformname")).contains("west"), "westbound")
            .when(lower(col("platformname")).contains("north"), "northbound")
            .when(lower(col("platformname")).contains("south"), "southbound")
            .otherwise(col("direction"))
        ).otherwise(col("direction"))
    )

    # METADATA
    df = df.withColumn("line_group", lit(line_group))

    # TRAIN TYPE
    df = df.withColumn(
        "train_type",
        when(col("vehicleid").isNotNull(), "real").otherwise("predicted")
    )

    # DEDUP
    df = df.dropDuplicates(["id", "stationname", "event_time"])

    cleaned_dfs.append(df)

# UNION ALL LINES
df_silver = reduce(align_and_union, cleaned_dfs)

# ENSURE COLUMN ORDER
for c in SILVER_COLS:
    if c not in df_silver.columns:
        df_silver = df_silver.withColumn(c, lit(None))

df_silver = df_silver.select(SILVER_COLS)

# WRITE SILVER
df_silver.coalesce(6).write.mode("overwrite").partitionBy("line_group").parquet(silver_path)

print("Silver Ready:", silver_path)
