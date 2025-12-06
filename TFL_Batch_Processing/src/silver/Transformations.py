# -*- coding: utf-8 -*-
"""
TFL SILVER LAYER TRANSFORMATION
Source: Hive External Tables backed by HDFS Raw CSV
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
    to_timestamp
)
from functools import reduce
from pyspark.sql.functions import to_timestamp, date_format, col, substring_index

# ============================================================
# SAFE UNION (Spark 2.4 compatible)
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
# SPARK SESSION (Hive enabled)
# ============================================================
spark = (
    SparkSession.builder
    .appName("TFL_Silver_Spark24")
    .enableHiveSupport()
    .getOrCreate()
)

# ============================================================
# HIVE BRONZE TABLES
# ============================================================
RAW_DB = "batchprocessing_tfl_db"

raw_tables = {
    "bakerloo":     "tfl_bakerloo_lines_bronze",
    "central":      "tfl_central_lines_bronze",
    "metropolitan": "tfl_metropolitan_lines_bronze",
    "northern":     "tfl_northern_lines_bronze",
    "piccadilly":   "tfl_piccadilly_lines_bronze",
    "victoria":     "tfl_victoria_lines_bronze"
}

# ============================================================
# SILVER CONFIG
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
# TRANSFORM EACH HIVE TABLE
# ============================================================
for line_group, table in raw_tables.items():
    print("Processing:", RAW_DB + "." + table)

    df = spark.table(RAW_DB + "." + table)

    # ---------------- STRING CLEAN ----------------
    df = (
        df.withColumn("stationname", trim(col("stationname")))
          .withColumn("linename", trim(col("linename")))
          .withColumn("platformname", trim(col("platformname")))
          .withColumn("direction", trim(col("direction")))
          .withColumn("destinationname", trim(col("destinationname")))
          .withColumn("currentlocation", trim(col("currentlocation")))
          .withColumn("towards", trim(col("towards")))
    )

    # ---------------- TIMESTAMPS ------------------
    df = df.withColumn(
    "event_time",
    when(
        col("timestamp_str").rlike("\\.\\d+Z$"),     # has fractional seconds
        to_timestamp(
            regexp_replace(col("timestamp_str"), "Z$", ""),
            "yyyy-MM-dd'T'HH:mm:ss.SSS"
        )
    ).otherwise(
        to_timestamp(
            regexp_replace(col("timestamp_str"), "Z$", ""),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    ))
    df = df.withColumn(
        "expectedarrival_ts",
        to_timestamp(
            regexp_replace(col("expectedarrival"), "Z$", ""),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )

    # ---------------- SAFE INT CAST ----------------
    df = df.withColumn(
        "timetostation",
        when(
            trim(col("timetostation")).rlike("^[0-9]+$"),
            col("timetostation").cast("int")
        ).otherwise(lit(None))
    )

    # ---------------- DIRECTION FIX ----------------
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

    # ---------------- TRAIN TYPE ------------------
    df = df.withColumn(
        "train_type",
        when(col("vehicleid").isNotNull(), "real").otherwise("predicted")
    )

    # ---------------- METADATA --------------------
    df = df.withColumn("line_group", lit(line_group))

    # ---------------- DEDUP -----------------------
    df = df.dropDuplicates(["id", "stationname", "event_time"])

    df = df.select([c for c in SILVER_COLS if c in df.columns])
    cleaned_dfs.append(df)

# ============================================================
# UNION + SCHEMA ENFORCEMENT
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
    df_silver.write
    .mode("overwrite")
    .partitionBy("line_group")
    .parquet(silver_path)
)

print("Silver layer created successfully")
print("Output:", silver_path)
