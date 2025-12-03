# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import yaml
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

with open("config/dev.yaml") as f:
    cfg = yaml.safe_load(f)

INCOMING_PATH = cfg["hdfs"]["incoming_path"]
ARCHIVE_PATH = cfg["paths"]["streaming_archive"]

def getSpark():
    return (
        SparkSession.builder
        .appName("UK_TFL_STREAM_ARCHIVE")
        .master("yarn")
        .getOrCreate()
    )

def get_fs(spark):
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def archive_files(fs, src_path, dst_path, spark):
    if not fs.exists(src_path):
        logging.info("incoming path does not exist: {}".format(src_path))
        return
    files = fs.listStatus(src_path)
    if not files:
        logging.info("incoming path empty")
        return
    for f in files:
        src = f.getPath()
        dst = spark._jvm.org.apache.hadoop.fs.Path("{}/{}".format(dst_path, src.getName()))
        logging.info("archiving {} -> {}".format(src, dst))
        fs.rename(src, dst)
    logging.info("archiving complete")

def delete_remaining(fs, path):
    if fs.exists(path):
        logging.info("deleting {}".format(path))
        fs.delete(path, True)
    else:
        logging.info("nothing to delete")

def main():
    spark = getSpark()
    fs = get_fs(spark)
    incoming = spark._jvm.org.apache.hadoop.fs.Path(INCOMING_PATH)
    archive_files(fs, incoming, ARCHIVE_PATH, spark)
    delete_remaining(fs, incoming)

if __name__ == "__main__":
    main()
