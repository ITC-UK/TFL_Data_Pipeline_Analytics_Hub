# -*- coding: utf-8 -*-

#import necessary lib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, from_unixtime, when, lit
from functools import reduce

# -------------------- SAFE UNION FUNCTION --------------------
def align_and_union(df1, df2):
    all_cols = list(set(df1.columns) | set(df2.columns))
    for c in all_cols:
        if c not in df1.columns:
            df1 = df1.withColumn(c, lit(None))
        if c not in df2.columns:
            df2 = df2.withColumn(c, lit(None))
    df1 = df1.select(sorted(all_cols))
    df2 = df2.select(sorted(all_cols))
    return df1.union(df2)

# -------------------- SPARK SESSION -------------------------
spark = (
    SparkSession.builder
    .appName("TFL_Silver_Transformation")
    .enableHiveSupport()
    .getOrCreate()
)

RAW_DB = "default"
SILVER_DB = "tfl_silver_db"

raw_tables = {
    "bakerloo": "tfl_bakerloo_lines_raw",
    "central": "tfl_central_lines_raw",
    "metropolitan": "tfl_metropolitan_lines_raw",
    "northern": "tfl_northern_lines_raw",
    "piccadilly": "tfl_piccadilly_lines_raw",
    "victoria": "tfl_victoria_lines_raw"
}

cleaned_dfs = []

for line_name, table in raw_tables.items():
    print("Processing " + table + "...")

    df = spark.table(RAW_DB + "." + table)

    # Trim text columns
    df = (
        df.withColumn("stationName", trim(col("stationName")))
          .withColumn("lineName", trim(col("lineName")))
          .withColumn("platformName", trim(col("platformName")))
          .withColumn("direction", trim(col("direction")))
          .withColumn("destinationName", trim(col("destinationName")))
          .withColumn("currentLocation", trim(col("currentLocation")))
          .withColumn("towards", trim(col("towards")))
    )

    # Convert timestamp
    df = df.withColumn("event_time", col("timestamp").cast("timestamp"))
    df = df.drop("timestamp")

    # Remove duplicate train observations
    df = df.dropDuplicates(["id", "stationName", "lineName", "event_time"])

    # Fix missing direction
    df = df.withColumn(
        "direction",
        when(col("direction").isNull() | (col("direction") == ""), 
             when(lower(col("platformName")).contains("eastbound"), lit("eastbound"))
            .when(lower(col("platformName")).contains("westbound"), lit("westbound"))
            .when(lower(col("platformName")).contains("northbound"), lit("northbound"))
            .when(lower(col("platformName")).contains("southbound"), lit("southbound"))
            .when(lower(col("platformName")).contains("inner rail"), lit("inner-rail"))
            .when(lower(col("platformName")).contains("outer rail"), lit("outer-rail"))
            .otherwise(None)
        ).otherwise(col("direction"))
    )

    # Real vs predicted
    df = df.withColumn(
        "train_type",
        when(col("vehicleid").isNotNull(), lit("real")).otherwise(lit("predicted"))
    )

    # Add line group
    df = df.withColumn("line_group", lit(line_name))

    # Select safe columns
    safe_cols = [
        "id", "stationName", "lineName", "line_group",
        "platformName", "direction", "destinationName",
        "event_time", "currentLocation", "towards", "train_type"
    ]

    df = df.select([c for c in safe_cols if c in df.columns])

    cleaned_dfs.append(df)

# -------------------- UNION ALL LINES ------------------------
df_silver = reduce(lambda a, b: align_and_union(a, b), cleaned_dfs)

# final consistent order
final_columns = [
    "id", "stationName", "lineName", "line_group",
    "platformName", "direction", "destinationName",
    "event_time", "currentLocation", "towards", "train_type"
]

df_silver = df_silver.select(final_columns)



#df_silver.to_csv("C:/Users/Kavya/OneDrive/Desktop/TFL_Project/TFL_Data_Pipeline_Analytics_Hub/TFL_Batch Processing/data/raw/silver", index=False)

# -------------------- WRITE TO hdfs --------------------------
output_path = "/tmp/DE011025/TFL_Batch_processing/silver-output2"

df_silver.write.mode("overwrite").csv(output_path)

print("Silver file saved to HDFS:", output_path)


print("Silver table created successfully!")







