#!/bin/bash

# Load .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo ".env file not found!"
    exit 1
fi

# PostgreSQL details from .env
hostName="$DB_HOST"
dbName="$DB_NAME"
userName="$DB_USERNAME"
password="$DB_PASSWORD"

# Hive database
HIVE_DATABASE="batchprocessing_tfl_db"

# HiveServer2 URL
HIVE_URL="jdbc:hive2://13.40.34.186:10000/${HIVE_DATABASE};"

# Base HDFS directory for raw data
BASE_HDFS="/tmp/DE011025/TFL_Batch_processing/raw"




