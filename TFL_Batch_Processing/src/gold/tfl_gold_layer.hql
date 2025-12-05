USE batchprocessing_tfl_db;

CREATE EXTERNAL TABLE IF NOT EXISTS tfl_gold_table5 (
    linename STRING,
    platformname STRING,
    direction STRING,
    destinationnaptanid STRING,
    destinationname STRING,
    event_time TIMESTAMP,
    timetostation INT,
    currentlocation STRING,
    towards STRING,
    expectedarrival_ts TIMESTAMP,
    train_type STRING
)
PARTITIONED BY (line_group STRING)
STORED AS PARQUET
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/tfl_silver_incremental';

MSCK REPAIR TABLE tfl_gold_table5;
