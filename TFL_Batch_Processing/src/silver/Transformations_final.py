# -*- coding: utf-8 -*-
"""
TFL SILVER TRANSFORMATION (PRODUCTION SAFE VERSION)
Handles:
 - Different column orders for each TFL line
 - Layout-aware renaming
 - Timestamp parsing
 - Schema alignment
 - Row-count reporting
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, when, lit,
    regexp_replace, to_timestamp
)
from functools import reduce

# ======================================================
#  PATHS
# ======================================================
BRONZE_BASE = "hdfs:///tmp/DE011025/TFL_Batch_processing/bronze"
SILVER_PATH = "hdfs:///tmp/DE011025/TFL_Batch_processing/tfl_silver_incremental1"

# ======================================================
#  LINE GROUPS
# ======================================================
line_groups = [
    "bakerloo",
    "central",
    "metropolitan",
    "northern",
    "piccadilly",
    "victoria"
]

# ======================================================
#  STANDARD SILVER OUTPUT SCHEMA
# ======================================================
SILVER_COLS = [
    "id","vehicleid","naptanid","stationname","lineid","linename","line_group",
    "platformname","direction","destinationnaptanid","destinationname",
    "event_time","timetostation","currentlocation","towards",
    "expectedarrival_ts","train_type"
]

# ======================================================
# COLUMN ORDER FOR EACH LINE GROUP
#  (MUST MATCH EXACT RAW CSV ORDER)
# ======================================================
COLUMN_LAYOUTS = {

    "bakerloo": [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","direction","bearing",
        "destinationnaptanid","destinationname","timestamp_str",
        "timetostation","currentlocation","towards","expectedarrival",
        "timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received","api_fetch_time"
    ],

    "central": [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","direction","bearing",
        "destinationnaptanid","destinationname","timestamp_str",
        "timetostation","currentlocation","towards","expectedarrival",
        "timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received","api_fetch_time"
    ],

    "metropolitan": [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","bearing",
        "timestamp_str","timetostation","currentlocation","towards",
        "expectedarrival","timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received",
        "destinationnaptanid","destinationname","direction","api_fetch_time"
    ],

    "northern": [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","direction","bearing",
        "destinationnaptanid","destinationname","timestamp_str",
        "timetostation","currentlocation","towards","expectedarrival",
        "timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received","api_fetch_time"
    ],

    "piccadilly": [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","direction","bearing",
        "destinationnaptanid","destinationname","timestamp_str",
        "timetostation","currentlocation","towards","expectedarrival",
        "timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received","api_fetch_time"
    ],

    "victoria": [
        "type","type2","id","operationtype","vehicleid","naptanid","stationname",
        "lineid","linename","platformname","bearing",
        "destinationnaptanid","destinationname","timestamp_str",
        "timetostation","currentlocation","towards","expectedarrival",
        "timetolive","modename","timing_type1","timing_type2",
        "timing_countdownserveradjustment","timing_source","timing_insert",
        "timing_read","timing_sent","timing_received",
        "direction","api_fetch_time"
    ]
}

# ======================================================
# SAFE UNION (Spark 2.4 Compatible)
# ======================================================
def align_and_union(df1, df2):
    cols = list(set(df1.columns) | set(df2.columns))
    for c in cols:
        if c not in df1.columns:
            df1 = df1.withColumn(c, lit(None))
        if c not in df2.columns:
            df2 = df2.withColumn(c, lit(None))
    return df1.select(sorted(cols)).union(df2.select(sorted(cols)))

# ======================================================
# SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("TFL_Silver_Full_Pipeline")
    .enableHiveSupport()
    .getOrCreate()
)

cleaned_dfs = []

print("\n================ SILVER TRANSFORMATION STARTED ================\n")

# ======================================================
# PROCESS EACH LINE GROUP
# ======================================================
for line_group in line_groups:
    print(f"\n--- Processing line group: {line_group} ---")

    layout = COLUMN_LAYOUTS[line_group]
    path = f"{BRONZE_BASE}/TFL_{line_group}_lines/run_*/*"

    print(f"Reading files from: {path}")

    df = (
        spark.read
        .option("header", "false")
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )

    # Fix columns to match expected layout
    raw_cols = df.columns
    if len(raw_cols) < len(layout):
        for i in range(len(raw_cols), len(layout)):
            df = df.withColumn(f"_missing_{i}", lit(None))
        raw_cols = df.columns

    df = df.select(raw_cols[:len(layout)]).toDF(*layout)

    # Clean strings
    for c in ["stationname", "linename", "platformname", "direction",
              "destinationname", "currentlocation", "towards"]:
        df = df.withColumn(c, trim(col(c)))

    # Timestamp parsing
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

    df = df.withColumn(
        "expectedarrival_ts",
        to_timestamp(
            regexp_replace(col("expectedarrival"), "Z$", ""),
            "yyyy-MM-dd'T'HH:mm:ss"
        )
    )

    # Cast int safely
    df = df.withColumn("id", col("id").cast("bigint"))
    df = df.withColumn("vehicleid", col("vehicleid").cast("int"))
    df = df.withColumn("naptanid", col("naptanid").cast("string"))
    df = df.withColumn(
        "timetostation",
        when(trim(col("timetostation").cast("string")).rlike("^[0-9]+$"),
             col("timetostation").cast("int"))
        .otherwise(lit(None))
    )
    
    # Direction fill
    df = df.withColumn(
        "direction",
        when(col("direction").isNull() | (col("direction") == ""),
             when(lower(col("platformname")).contains("east"), "eastbound")
             .when(lower(col("platformname")).contains("west"), "westbound")
             .when(lower(col("platformname")).contains("north"), "northbound")
             .when(lower(col("platformname")).contains("south"), "southbound")
             .otherwise(col("direction"))
        ).otherwise(col("direction"))
    )

    df = df.withColumn("train_type",
                       when(col("vehicleid").isNotNull(), "real")
                       .otherwise("predicted"))

    df = df.withColumn("line_group", lit(line_group))

    df = df.dropDuplicates(["id", "stationname", "event_time"])

    row_count = df.count()
    print(f"✔ {line_group} rows after cleaning: {row_count}")

    cleaned_dfs.append(df)

# ======================================================
# UNION & FINAL SCHEMA ENFORCEMENT
# ======================================================
df_silver = reduce(align_and_union, cleaned_dfs)

for c in SILVER_COLS:
    if c not in df_silver.columns:
        df_silver = df_silver.withColumn(c, lit(None))

df_silver = df_silver.select(SILVER_COLS)

# ======================================================
# WRITE SILVER
# ======================================================
df_silver.coalesce(6).write.mode("overwrite") \
    .partitionBy("line_group") \
    .parquet(SILVER_PATH)

print("\n================ SILVER TRANSFORMATION COMPLETED ================\n")
print("Output written to:", SILVER_PATH)
