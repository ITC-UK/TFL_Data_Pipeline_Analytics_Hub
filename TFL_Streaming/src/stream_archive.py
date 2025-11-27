from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder \
    .appName("UK_TFL_ARCHIVE_INCOMING") \
    .getOrCreate()

# hdfs paths
incoming_path = "hdfs:////tmp/DE011025/uk/streaming/incoming/"
archive_path = "/tmp/DE011025/uk/streaming/archive"

# get hadoop fs
hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
hadoop_incoming = spark._jvm.org.apache.hadoop.fs.Path(incoming_path)
hadoop_archive = spark._jvm.org.apache.hadoop.fs.Path(archive_path)

# move files to archive
if hadoop_fs.exists(hadoop_incoming):
    files = hadoop_fs.listStatus(hadoop_incoming)
    for f in files:
        src = f.getPath()
        dst = spark._jvm.org.apache.hadoop.fs.Path(archive_path + "/" + src.getName())
        hadoop_fs.rename(src, dst)
    print("archived incoming parquet files")

# delete any remaining files in incoming (safety)
if hadoop_fs.exists(hadoop_incoming):
    hadoop_fs.delete(hadoop_incoming, True)
    print("deleted remaining files in incoming")
