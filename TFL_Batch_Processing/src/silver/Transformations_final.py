# -*- coding: utf-8 -*-
"""
TFL SILVER TRANSFORMATION
Reads ALL bronze run folders (full + incremental)
Automatically detects column order from first row
Handles schema drift safely
Works with any part/part-* file
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, lit,
    regexp_replace, to_timestamp
)
from functools import reduce

# -------------------------------------------------------------------
# BRONZE + SILVER PATHS
# -------------------------------------------------------------------
BRONZE_BASE = "hdfs:///tmp/DE011025/TFL_Batch_processing/bronze"
SILVER_PATH = "hdfs:///tmp/DE011025/TFL_Batch_processing/tfl_silver_incremental"

line_groups = [
    "bakerloo", "central", "metropolitan",
    "northern", "piccadilly", "victoria"
]

# -------------------------------------------------------------------
# EXPECTED COLUMN NAMES (official TFL API structure)
# -------------------------------------------------------------------
EXPECTED_COLS = [
    "type", "type2", "id", "operationtype", "vehicleid", "naptanid",
    "stationname", "lineid", "linename", "platformname", "direction",
    "bearing", "destinationnaptanid", "destinationname",
    "timestamp_str", "timetostation", "currentlocation", "towards",
    "expectedarrival", "timetolive", "modename", "timing_type1",
    "timing_type2", "timing_countdownserveradjustment", "timing_source",
    "timing_insert", "timing_read", "timing_sent", "timing_received",
    "api_fetch_time"
]

SILVER_COLS = [
    "id","vehicleid","naptanid","stationname","lineid","linename",
    "line_group","platformname","direction","destinationnaptanid",
    "destinationname","event_time","timetostation","currentlocation",
    "towards","expectedarrival_ts","train_type"
]

# -------------------------------------------------------------------
# AUTO-REORDER COLUMNS BASED ON EXPECTED STRUCTURE
# -------------------------------------------------------------------
def reorder_columns(df):
    """
    Spark infers schema differently for each file.
    This aligns columns to expected structure using index position.
    """
    current_cols = df.columns
    mapping = {}

    for i, actual_col in enumerate(current_cols):
        if i < len(EXPECTED_COLS):
            mapping[actual_col] = EXPECTED_COLS[i]

    # Apply renaming
    for actual, expected in mapping.items():
        df = df.withColumnRenamed(actual, expected)

    return df

# -------------------------------------------------------------------
# UNION WITH SCHEMA ALIGNMENT
# -------------------------------------------------------------------
def align_and_union(df1, df2):
    all_cols = list(set(df1.columns) | set(df2.columns))
    for c in all_cols:
        if c not in df1.columns:
            df1 = df1.withColumn(c, lit(None))
        if c not in df2.columns:
            df2 = df2.withColumn(c, lit(None))
    return df1.select(sorted(all_cols)).union(df2.select(sorted(all_cols)))

# -------------------------------------------------------------------
# START SPARK
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TFL_Silver_Transformation_V2")
    .enableHiveSupport()
    .getOrCreate()
)

cleaned_dfs = []

# -------------------------------------------------------------------
# PROCESS EACH LINE GROUP
# -------------------------------------------------------------------
for line_group in line_groups:

    path = f"{BRONZE_BASE}/TFL_{line_group}_lines/run_*/*"
    print("\n📌 Reading Bronze Files:", path)

    df = (
        spark.read
        .option("header", "false")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )

    # Reorder based on expected TFL structure
    df = reorder_columns(df)

    # Clean strings
    df = (
        df.withColumn("stationname", trim(col("stationname")))
          .withColumn("linename", trim(col("linename")))
          .withColumn("platformname", trim(col("platformname")))
          .withColumn("direction", trim(col("direction")))
          .withColumn("destinationname", trim(col("destinationname")))
          .withColumn("currentlocation", trim(col("currentlocation")))
          .withColumn("towards", trim(col("towards")))
    )

    # EVENT TIME FIX
    df = df.withColumn(
        "event_time",
        when(col("timestamp_str").rlike("\\.\\d+Z$"),
             to_timestamp(regexp_replace(col("timestamp_str"), "Z$", ""),
                          "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .otherwise(
             to_timestamp(regexp_replace(col("timestamp_str"), "Z$", ""),
                          "yyyy-MM-dd'T'HH:mm:ss")
        )
    )

    # EXPECTED ARRIVAL FIX (IMPORTANT!)
    df = df.withColumn(
        "expectedarrival_ts",
        when(col("expectedarrival").rlike("\\.\\d+Z$"),
             to_timestamp(regexp_replace(col("expectedarrival"), "Z$", ""),
                          "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .otherwise(
             to_timestamp(regexp_replace(col("expectedarrival"), "Z$", ""),
                          "yyyy-MM-dd'T'HH:mm:ss")
        )
    )

    # FIX TIMETOSTATION
    df = df.withColumn(
        "timetostation",
        when(trim(col("timetostation").cast("string")).rlike("^[0-9]+$"),
             col("timetostation").cast("int"))
        .otherwise(lit(None))
    )

    # HANDLE MISSING DIRECTION
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

# -------------------------------------------------------------------
# MERGE ALL LINES INTO ONE SILVER TABLE
# -------------------------------------------------------------------
df_silver = reduce(align_and_union, cleaned_dfs)

# ENFORCE FINAL COLUMN ORDER
for c in SILVER_COLS:
    if c not in df_silver.columns:
        df_silver = df_silver.withColumn(c, lit(None))

df_silver = df_silver.select(SILVER_COLS)

# -------------------------------------------------------------------
# WRITE SILVER
# -------------------------------------------------------------------
df_silver.coalesce(6).write.mode("overwrite").partitionBy("line_group").parquet(SILVER_PATH)

print("\n🌟 SILVER LAYER READY!")
print("📂 Output:", SILVER_PATH)
