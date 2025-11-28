CREATE DATABASE batchprocessing_tfl_db; 
USE batchprocessing_tfl_db;


CREATE EXTERNAL TABLE IF NOT EXISTS tfl_bakerloo_lines_raw (
  type STRING,
  id STRING,
  operationType INT,
  vehicleId INT,
  naptanId STRING,
  stationName STRING,
  lineId STRING,
  lineName STRING,
  platformName STRING,
  direction STRING,
  destinationNaptanId STRING,
  destinationName STRING,
  `timestamp` BIGINT,
  timeToStation INT,
  currentLocation STRING,
  towards STRING,
  expectedArrival STRING,
  tubeTravelTime INT,
  timeToArrival INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/raw/TFL_bakerloo_lines';

select * from tfl_central_lines_raw;

CREATE EXTERNAL TABLE IF NOT EXISTS tfl_central_lines_raw (
  type STRING,
  id STRING,
  operationType INT,
  vehicleId INT,
  naptanId STRING,
  stationName STRING,
  lineId STRING,
  lineName STRING,
  platformName STRING,
  direction STRING,
  destinationNaptanId STRING,
  destinationName STRING,
  `timestamp` BIGINT,
  timeToStation INT,
  currentLocation STRING,
  towards STRING,
  expectedArrival STRING,
  tubeTravelTime INT,
  timeToArrival INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/raw/TFL_central_lines';


CREATE EXTERNAL TABLE IF NOT EXISTS tfl_metropolitan_lines_raw (
  type STRING,
  id STRING,
  operationType INT,
  vehicleId INT,
  naptanId STRING,
  stationName STRING,
  lineId STRING,
  lineName STRING,
  platformName STRING,
  direction STRING,
  destinationNaptanId STRING,
  destinationName STRING,
  `timestamp` BIGINT,
  timeToStation INT,
  currentLocation STRING,
  towards STRING,
  expectedArrival STRING,
  tubeTravelTime INT,
  timeToArrival INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/raw/TFL_metropolitan_lines';


CREATE EXTERNAL TABLE IF NOT EXISTS tfl_northern_lines_raw (
  type STRING,
  id STRING,
  operationType INT,
  vehicleId INT,
  naptanId STRING,
  stationName STRING,
  lineId STRING,
  lineName STRING,
  platformName STRING,
  direction STRING,
  destinationNaptanId STRING,
  destinationName STRING,
  `timestamp` BIGINT,
  timeToStation INT,
  currentLocation STRING,
  towards STRING,
  expectedArrival STRING,
  tubeTravelTime INT,
  timeToArrival INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/raw/TFL_northern_lines';


CREATE EXTERNAL TABLE IF NOT EXISTS tfl_piccadilly_lines_raw (
  type STRING,
  id STRING,
  operationType INT,
  vehicleId INT,
  naptanId STRING,
  stationName STRING,
  lineId STRING,
  lineName STRING,
  platformName STRING,
  direction STRING,
  destinationNaptanId STRING,
  destinationName STRING,
  `timestamp` BIGINT,
  timeToStation INT,
  currentLocation STRING,
  towards STRING,
  expectedArrival STRING,
  tubeTravelTime INT,
  timeToArrival INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/raw/TFL_piccadilly_lines';


CREATE EXTERNAL TABLE IF NOT EXISTS tfl_victoria_lines_raw (
  type STRING,
  id STRING,
  operationType INT,
  vehicleId INT,
  naptanId STRING,
  stationName STRING,
  lineId STRING,
  lineName STRING,
  platformName STRING,
  direction STRING,
  destinationNaptanId STRING,
  destinationName STRING,
  `timestamp` BIGINT,
  timeToStation INT,
  currentLocation STRING,
  towards STRING,
  expectedArrival STRING,
  tubeTravelTime INT,
  timeToArrival INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION 'hdfs:///tmp/DE011025/TFL_Batch_processing/raw/TFL_victoria_lines';

SHOW TABLES;
