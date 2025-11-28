#import necessary lib

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, from_unixtime, when, length, lit
)
from pyspark.sql.types import *
from functools import reduce

#create spark session 

spark = (
    SparkSession.builder
    .appName("TFL_Silver_Transformation")
    .enableHiveSupport()
    .getOrCreate())

#Configure 
RAW_DB = "default"
SILVER_DB = "tfl_silver_db"


#create one dir for all tables and load them
#List of RAW tables to load (key- short name & value actual hive table)

raw_tables = {
    "bakerloo": "tfl_bakerloo_lines_raw",
    "central": "tfl_central_lines_raw",
    "metropolitan": "tfl_metropolitan_lines_raw",
    "northern": "tfl_northern_lines_raw",
    "piccadilly": "tfl_piccadilly_lines_raw",
    "victoria": "tfl_victoria_lines_raw"}

#create empty list - store each cleaned DataFrames
cleaned_dfs = []

#loop over each table print helps for logs which table working on.

for line_name, table in raw_tables.items():
    print(f"Processing {table}...")

#read hive tables intp pyspark DataFrame

df = spark.table(f"{RAW_DB}.{table}")

# drop null col if less than 30% are non-null → mostly empty → DROP it
total_count = df.count()
df = df.drop(*[
    c for c in df.columns
    if df.filter(col(c).isNotNull()).count() < total_count * 0.3])

#Convert Timestamp to Readable Datetime
# in raw time stamp in millisec - from unixtime need seconds
#created new col event_time = readable datetime
df = df.withColumn(
    "event_time",
    from_unixtime(col("timestamp") / 1000))

#then remove old timestamp
df = df.drop("timestamp")

#Remove LOGICAL Duplicates - removes same tarin, same station, on same line, at same datetime
df = df.dropDuplicates(["id", "stationName", "lineName", "event_time"])

#String Cleaning 
#Add line → which line (bakerloo, central…)
#trim() removes spaces
#lower() makes everything consistent

df = (
    df.withColumn("line_group", lit(line_name))
      .withColumn("stationName", trim(col("stationName")))
      .withColumn("lineName", lower(trim(col("lineName"))))
      .withColumn("platformName", trim(col("platformName")))
      .withColumn("direction", trim(col("direction")))
      .withColumn("destinationName", trim(col("destinationName")))
      .withColumn("currentLocation", trim(col("currentLocation")))
      .withColumn("towards", trim(col("towards"))))

# adding new col becoz train type (real and predicted having - values)

df = df.withColumn(
    "train_type",
    when(col("id") > 0, lit("real"))
    .otherwise(lit("predicted")))

#store cleaned dataframe into an empty list created before
cleaned_dfs.append(df)

#Union All Six Lines Into One Silver DataFrame
#reduce() → takes the first two DFs, unions them--Then union result with third--Then 4th → until all 6 are combined
#unionByName() aligns columns by name
#allowMissingColumns=True fills missing columns with nulls

df_silver = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), cleaned_dfs)

#Reorder Columns for Clean Output - only required coloumns
final_columns = [
    "id", "operationType", "vehicleId", "naptanId", "stationName", "lineId",
    "lineName", "line_group", "platformName", "direction",
    "destinationNaptanId", "destinationName", "event_time",
    "timeToStation", "currentLocation", "towards", "expectedArrival",
    "tubeTravelTime", "timeToArrival", "train_type"
]

#select columns in oder 
df_silver = df_silver.select([c for c in final_columns if c in df_silver.columns])

#Write Silver table to Hive

df_silver.write.mode("overwrite").format("parquet").saveAsTable(
    f"{SILVER_DB}.tfl_tube_arrivals_silver")

print("Silver table created successfully!")






