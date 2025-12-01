--create database
CREATE DATABASE IF NOT EXISTS tfl_gold_db;
USE tfl_gold_db;

--------- DIMENSION TABLES-------
-- dim_line

CREATE TABLE dim_line AS
SELECT
    ROW_NUMBER() OVER (ORDER BY lineId) AS line_key,
    lineId,
    lineName,
    line_group
FROM (
    SELECT DISTINCT lineId, lineName, line_group
    FROM tfl_silver_db.tfl_tube_arrivals_silver
) l;

--dim_station

CREATE TABLE dim_station AS
SELECT
    ROW_NUMBER() OVER (ORDER BY naptanId) AS station_key,
    naptanId,
    stationName,
    platformName,
    direction
FROM (
    SELECT DISTINCT naptanId, stationName, platformName, direction
    FROM tfl_silver_db.tfl_tube_arrivals_silver
) s;

--dim_destination

CREATE TABLE dim_destination AS
SELECT
    ROW_NUMBER() OVER (ORDER BY destinationNaptanId) AS destination_key,
    destinationNaptanId,
    destinationName,
    towards
FROM (
    SELECT DISTINCT destinationNaptanId, destinationName, towards
    FROM tfl_silver_db.tfl_tube_arrivals_silver
) d;

-- dim_vehicle

CREATE TABLE dim_vehicle AS
SELECT
    ROW_NUMBER() OVER (ORDER BY vehicleId) AS vehicle_key,
    vehicleId,
    train_type
FROM (
    SELECT DISTINCT vehicleId, train_type
    FROM tfl_silver_db.tfl_tube_arrivals_silver
) v;


--dim_date

CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY full_date) AS date_key,
    full_date,
    year(full_date) AS year,
    quarter(full_date) AS quarter,
    month(full_date) AS month,
    day(full_date) AS day,
    hour(event_time) AS hour,
    date_format(full_date, 'EEEE') AS weekday_name,
    CASE WHEN date_format(full_date, 'u') IN ('6','7') 
         THEN 'yes' ELSE 'no' END AS is_weekend,
    CASE WHEN hour(event_time) BETWEEN 7 AND 10 
           OR hour(event_time) BETWEEN 16 AND 19
         THEN 'yes' ELSE 'no' END AS is_peak_hour
FROM (
    SELECT DISTINCT 
        to_date(event_time) AS full_date,
        event_time
    FROM tfl_silver_db.tfl_tube_arrivals_silver
) dt;


--------- FACT TABLE-------

CREATE TABLE fact_tube_arrivals AS
SELECT
    ROW_NUMBER() OVER (ORDER BY event_time) AS fact_key,

    -- foreign keys
    s.station_key,
    l.line_key,
    d.destination_key,
    v.vehicle_key,
    dt.date_key,

    -- measures
    sil.event_time,
    sil.timeToStation,
    sil.currentLocation,
    sil.expectedArrival

FROM tfl_silver_db.tfl_tube_arrivals_silver sil

LEFT JOIN dim_station s
    ON sil.naptanId = s.naptanId

LEFT JOIN dim_line l
    ON sil.lineId = l.lineId

LEFT JOIN dim_destination d
    ON sil.destinationNaptanId = d.destinationNaptanId

LEFT JOIN dim_vehicle v
    ON sil.vehicleId = v.vehicleId

LEFT JOIN dim_date dt
    ON to_date(sil.event_time) = dt.full_date;

FROM tfl_silver_db.tfl_tube_arrivals_silver sil
