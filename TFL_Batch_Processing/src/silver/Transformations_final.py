from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, lit, regexp_replace, to_timestamp
from functools import reduce

BRONZE_BASE = "hdfs:///tmp/DE011025/TFL_Batch_processing/bronze"

line_groups = ["bakerloo","central","metropolitan","northern","piccadilly","victoria"]

silver_path = "hdfs:///tmp/DE011025/TFL_Batch_processing/tfl_silver_incremental"

SILVER_COLS = [
    "id","vehicleid","naptanid","stationname","lineid","linename","line_group",
    "platformname","direction","destinationnaptanid","destinationname",
    "event_time","timetostation","currentlocation","towards",
    "expectedarrival_ts","train_type"
]

raw_cols = [
    "type","type2","id","operationtype","vehicleid","naptanid","stationname",
    "lineid","linename","platformname","direction","bearing",
    "destinationnaptanid","destinationname","timestamp_str",
    "timetostation","currentlocation","towards",
    "expectedarrival","timetolive","modename",
    "timing_type1","timing_type2","timing_countdownserveradjustment",
    "timing_source","timing_insert","timing_read","timing_sent",
    "timing_received","api_fetch_time"
]

def align_and_union(df1, df2):
    cols = list(set(df1.columns) | set(df2.columns))
    for c in cols:
        if c not in df1.columns:
            df1 = df1.withColumn(c, lit(None))
        if c not in df2.columns:
            df2 = df2.withColumn(c, lit(None))
    return df1.select(sorted(cols)).union(df2.select(sorted(cols)))

spark = SparkSession.builder.appName("Silver").getOrCreate()

cleaned_dfs = []

for line_group in line_groups:
    path = f"{BRONZE_BASE}/TFL_{line_group}_lines/run_*/*"

    df = spark.read.option("header","false").option("inferSchema","true").csv(path)

    # Rename raw CSV columns
    for i, c in enumerate(df.columns):
        df = df.withColumnRenamed(c, raw_cols[i])

    # Clean strings
    df = (
        df.withColumn("stationname", trim(col("stationname")))
          .withColumn("linename", trim(col("linename")))
          .withColumn("platformname", trim(col("platformname")))
          .withColumn("destinationname", trim(col("destinationname")))
          .withColumn("currentlocation", trim(col("currentlocation")))
          .withColumn("towards", trim(col("towards")))
    )

    # Convert timestamps
    df = df.withColumn(
        "event_time",
        to_timestamp(regexp_replace(col("timestamp_str"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )

    df = df.withColumn(
        "expectedarrival_ts",
        to_timestamp(regexp_replace(col("expectedarrival"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss")
    )

    # Safe cast
    df = df.withColumn(
        "timetostation",
        when(col("timetostation").cast("string").rlike("^[0-9]+$"),
             col("timetostation").cast("int")).otherwise(None)
    )

    # Train type
    df = df.withColumn("train_type", when(col("vehicleid").isNotNull(), "real").otherwise("predicted"))

    df = df.withColumn("line_group", lit(line_group))

    df = df.dropDuplicates(["id","stationname","event_time"])

    cleaned_dfs.append(df)

# Merge all lines
df_silver = reduce(align_and_union, cleaned_dfs)

# Ensure column order
df_silver = df_silver.select(SILVER_COLS)

df_silver.coalesce(6).write.mode("overwrite").partitionBy("line_group").parquet(silver_path)

print("SILVER READY:", silver_path)
