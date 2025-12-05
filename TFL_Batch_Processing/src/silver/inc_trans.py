# -*- coding: utf-8 -*-
"""
TFL SILVER LAYER TRANSFORMATION
Source: HDFS Raw CSV (incremental folders supported)
Target: Silver Parquet (partitioned by line_group)
Compatible with: Python 3.6 + Spark 2.4 (CDH)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    lit,
    regexp_replace,
    to_timestamp,
    coalesce
)
from functools import reduce

# ============================================================
# RAW CSV COLUMN NAMES (FIRST 24 ONLY)
# ============================================================
RAW_COLUMNS = [
    "type",
    "type2",
    "id",
    "operationtype",
    "vehicleid",
    "naptanid",
    "stationname",
    "lineid",
    "linename",
    "platformname",
    "direction",
    "bearing",
    "destinationnaptanid",
    "destinationname",
    "timestamp_str",
    "timetostation",
    "currentlocation",
    "towards",
    "expectedarrival",
    "timetolive",
    "modename",
    "timing_type1",
    "timing_type2",
    "timing_countdownserveradjustment"
]

EXPECTED_COLS = len(RAW_COLUMNS)

# ============================================================
# SAFE UNION (Spark 2.4 – no unionByName)
# ============================================================
def align_and_union(df1, df2):
    all_cols = list(set(df1.columns) | set(df2.columns))

    for c in all_cols:
        if c not in df1.columns:
            df1 = df1.withColumn(c, lit(None))
        if c not in df2.columns:
            df2 = df2.withColumn(c, lit(None))

    return df1.select(sorted(all_cols)).union(df2.select(sorted(all_cols)))

# ============================================================
# SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder
    .appName("TFL_Silver_HDFS_Based")
    .getOrCreate()
)

# ============================================================
# RAW INPUT PATHS (BASE + INCREMENTAL run_* SUPPORT)
# ============================================================
raw_base = "hdfs://ip-172-31-3-80.eu-west-2.compute.internal:8020/tmp/DE011025/TFL_Batch_processing/bronze"

raw_paths = {
    "bakerloo": [raw_base + "/TFL_bakerloo_lines/"],
    "central": [raw_base + "/TFL_central_lines/"],
    "metropolitan": [raw_base + "/TFL_metropolitan_lines/"],
    "northern": [raw_base + "/TFL_northern_lines/"],
    "piccadilly": [raw_base + "/TFL_piccadilly_lines/"],
    "victoria": [raw_base + "/TFL_victoria_lines/"],
}


# ============================================================
# SILVER OUTPUT CONFIG
# ============================================================

silver_path = "hdfs:///tmp/DE011025/TFL_Batch_processing/tfl_silver_incremental"



SILVER_COLS = [
    "id",
    "vehicleid",
    "naptanid",
    "stationname",
    "lineid",
    "linename",
    "line_group",
    "platformname",
    "direction",
    "destinationnaptanid",
    "destinationname",
    "event_time",
    "timetostation",
    "currentlocation",
    "towards",
    "expectedarrival_ts",
    "train_type"
]

cleaned_dfs = []

# ============================================================
# PROCESS EACH LINE (INCREMENTAL SAFE)
# ============================================================
for line_group, paths in raw_paths.items():
    print("Processing:", line_group)

    df = (
            spark.read \
            .option("header", "false") \
            .option("recursiveFileLookup", "true") \
            .csv(paths)

    )

    # --------------------------------------------------------
    # KEEP ONLY FIRST 24 COLUMNS
    # --------------------------------------------------------
    df = df.select(df.columns[:EXPECTED_COLS]).toDF(*RAW_COLUMNS)

    # ---------------- TEXT CLEANING -------------------------
    df = (
        df.withColumn("stationname", trim(col("stationname")))
          .withColumn("linename", trim(col("linename")))
          .withColumn("platformname", trim(col("platformname")))
          .withColumn("direction", trim(col("direction")))
          .withColumn("destinationname", trim(col("destinationname")))
          .withColumn("currentlocation", trim(col("currentlocation")))
          .withColumn("towards", trim(col("towards")))
    )

    # ---------------- TIMESTAMP FIX (IMPORTANT) -------------
    df = df.withColumn(
        "event_time",
        coalesce(
            to_timestamp(
                regexp_replace(col("timestamp_str"), "\\.\\d+Z$", ""),
                "yyyy-MM-dd'T'HH:mm:ss"
            ),
            to_timestamp(col("timestamp_str"), "yyyy-MM-dd HH:mm:ss")
        )
    )

    df = df.withColumn(
        "expectedarrival_ts",
        coalesce(
            to_timestamp(
                regexp_replace(col("expectedarrival"), "Z$", ""),
                "yyyy-MM-dd'T'HH:mm:ss"
            ),
            to_timestamp(col("expectedarrival"), "yyyy-MM-dd HH:mm:ss")
        )
    )

    # ---------------- FIX DIRECTION -------------------------
    df = df.withColumn(
        "direction",
        when(
            col("direction").isNull() | (col("direction") == ""),
            when(lower(col("platformname")).contains("east"),  "eastbound")
            .when(lower(col("platformname")).contains("west"),  "westbound")
            .when(lower(col("platformname")).contains("north"), "northbound")
            .when(lower(col("platformname")).contains("south"), "southbound")
            .otherwise(col("direction"))
        ).otherwise(col("direction"))
    )

    # ---------------- TRAIN TYPE ----------------------------
    df = df.withColumn(
        "train_type",
        when(col("vehicleid").isNotNull(), "real").otherwise("predicted")
    )

    # ---------------- LINE GROUP ----------------------------
    df = df.withColumn("line_group", lit(line_group))

    # ---------------- DEDUP ---------------------------------
    df = df.dropDuplicates(["id", "stationname", "event_time"])

    # ---------------- SELECT SILVER COLS --------------------
    df = df.withColumn(
    "timetostation",
    when(trim(col("timetostation")) == "", lit(None).cast("string"))
    .otherwise(col("timetostation").cast("string")))


    df = df.select([c for c in SILVER_COLS if c in df.columns])

    cleaned_dfs.append(df)

# ============================================================
# UNION ALL LINES
# ============================================================
df_silver = reduce(align_and_union, cleaned_dfs)

for c in SILVER_COLS:
    if c not in df_silver.columns:
        df_silver = df_silver.withColumn(c, lit(None))

df_silver = df_silver.select(SILVER_COLS).coalesce(6)

# ============================================================
# WRITE SILVER
# ============================================================
(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("line_group")
    .parquet(silver_path)
)

print("SILVER LAYER CREATED SUCCESSFULLY")
print("Location:", silver_path)
