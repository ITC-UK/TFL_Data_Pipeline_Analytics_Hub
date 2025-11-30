#!/bin/bash

# ---------------------------------------------------------
# UNIVERSAL INCREMENTAL SQOOP IMPORT SCRIPT FOR ALL TFL LINES
# ---------------------------------------------------------

# PostgreSQL details
hostName="18.134.163.221"
dbName="testdb"
userName="Consultants"
password="WelcomeItc@2022"

# Hive database
HIVE_DATABASE="batchprocessing_tfl_db"

# HiveServer2 URL (update port if needed)
HIVE_URL="jdbc:hive2://13.40.34.186:10000/${HIVE_DATABASE};"

# Base HDFS directory for raw data
BASE_HDFS="/tmp/DE011025/TFL_Batch_processing/raw"

# List of TFL tables (Postgres + Hive naming aligned)
declare -a TFL_LINES=(
    "bakerloo"
    "central"
    "metropolitan"
    "northern"
    "piccadilly"
    "victoria"
)

echo "--------------------------------------------------------"
echo "   STARTING UNIVERSAL INCREMENTAL INGESTION FOR ALL LINES"
echo "--------------------------------------------------------"

# Loop through each TFL line
for LINE in "${TFL_LINES[@]}"
do
    echo ""
    echo "============================="
    echo "Processing line: $LINE"
    echo "============================="

    # Table names
    PG_TABLE="TFL_${LINE}_lines"
    HIVE_TABLE="tfl_${LINE}_lines_raw"

    # HDFS path for this line
    LINE_HDFS="${BASE_HDFS}/TFL_${LINE}_lines"

    echo "Using PostgreSQL table: ${PG_TABLE}"
    echo "Using Hive table: ${HIVE_TABLE}"
    echo "Using HDFS path: ${LINE_HDFS}"

    # Ensure HDFS directory exists
    sudo -u hdfs hdfs dfs -mkdir -p ${LINE_HDFS}
    sudo -u hdfs hdfs dfs -chmod -R 777 ${LINE_HDFS}

    # Get last timestamp from Hive
    echo "Fetching last timestamp from Hive..."

    lastValue=$(beeline -u "${HIVE_URL}" --silent=true --showHeader=false --outputformat=tsv2 -e \
    "SELECT COALESCE(MAX(timestamp), 0) FROM ${HIVE_TABLE};" | tail -n 1)

    if [ -z "$lastValue" ]; then
        echo " ERROR: Failed to fetch last timestamp for ${HIVE_TABLE}"
        continue
    fi

    echo "Last timestamp in Hive = $lastValue"
    echo "Running incremental import..."

    # Sqoop incremental append
    sqoop import \
      --connect jdbc:postgresql://${hostName}:5432/${dbName} \
      --username ${userName} \
      --password ${password} \
      --table ${PG_TABLE} \
      --incremental append \
      --check-column timestamp \
      --last-value "${lastValue}" \
      --target-dir ${LINE_HDFS}/inc_$(date +%Y%m%d%H%M%S) \
      --m 1 \
      --as-textfile

    if [ $? -ne 0 ]; then
        echo " Sqoop import FAILED for ${PG_TABLE}"
        continue
    fi

    echo "Sqoop import successful for ${PG_TABLE}"
    echo "Updating Hive table partitions..."

    beeline -u "${HIVE_URL}" -e "MSCK REPAIR TABLE ${HIVE_TABLE};"

done

echo ""
echo " UNIVERSAL INCREMENTAL INGESTION COMPLETED SUCCESSFULLY!"
