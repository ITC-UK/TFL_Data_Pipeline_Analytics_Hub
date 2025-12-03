# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import yaml
import os

with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

KAFKA_SERVER = cfg["kafka"]["bootstrap_servers"]
TOPIC = cfg["kafka"]["topic"]
POLL_INTERVAL = cfg["tfl"]["polling_interval"]

TFL_APP_ID = os.getenv("TFL_APP_ID")
TFL_APP_KEY = os.getenv("TFL_APP_KEY")
API_LIST = cfg["tfl"]["api_list"]
CHECKPOINT_PATH = cfg["hdfs"]["checkpoint_path"]
INCOMING_PATH = cfg["hdfs"]["incoming_path"]

def getSpark():
    return SparkSession.builder.appName("UK_TFL_STREAM_ARCHIVE").getOrCreate()

def get_hadoop_fs():
    spark = getSpark()
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def archive_files(fs, src_path, dst_path):
    if fs.exists(src_path):
        files = fs.listStatus(src_path)
        for f in files:
            src = f.getPath()
            dst = fs.makeQualified(fs.getUri(), fs.getWorkingDirectory())
            dst = fs._jvm.org.apache.hadoop.fs.Path(f"{dst_path}/{src.getName()}")
            fs.rename(src, dst)
        print("Archived incoming parquet files")

def delete_remaining(fs, path):
    if fs.exists(path):
        fs.delete(path, True)
        print("Deleted remaining files in incoming")

def main():
    spark = getSpark()
    incoming_path = "hdfs:///tmp/DE011025/uk/streaming/incoming/"
    archive_path = "/tmp/DE011025/uk/streaming/archive"

    fs = get_hadoop_fs()
    hadoop_incoming = spark._jvm.org.apache.hadoop.fs.Path(incoming_path)

    archive_files(fs, hadoop_incoming, archive_path)
    delete_remaining(fs, ha_
