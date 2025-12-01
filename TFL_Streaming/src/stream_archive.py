# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

def create_spark_session(app_name="UK_TFL_ARCHIVE_INCOMING"):
    """Create and return a SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_hadoop_fs(spark):
    """Return Hadoop FileSystem object from SparkSession."""
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def archive_files(fs, src_path, dst_path):
    """Move all files from src_path to dst_path in HDFS."""
    if fs.exists(src_path):
        files = fs.listStatus(src_path)
        for f in files:
            src = f.getPath()
            dst = spark._jvm.org.apache.hadoop.fs.Path(f"{dst_path}/{src.getName()}")
            fs.rename(src, dst)
        print("Archived incoming parquet files")

def delete_remaining(fs, path):
    """Delete path in HDFS if it exists."""
    if fs.exists(path):
        fs.delete(path, True)
        print("Deleted remaining files in incoming")

def main():
    """Archive incoming parquet files and clean up HDFS path."""
    incoming_path = "hdfs:///tmp/DE011025/uk/streaming/incoming/"
    archive_path = "/tmp/DE011025/uk/streaming/archive"

    spark = create_spark_session()
    fs = get_hadoop_fs(spark)
    hadoop_incoming = spark._jvm.org.apache.hadoop.fs.Path(incoming_path)
    hadoop_archive = spark._jvm.org.apache.hadoop.fs.Path(archive_path)

    archive_files(fs, hadoop_incoming, archive_path)
    delete_remaining(fs, hadoop_incoming)

if __name__ == "__main__":
    main()
