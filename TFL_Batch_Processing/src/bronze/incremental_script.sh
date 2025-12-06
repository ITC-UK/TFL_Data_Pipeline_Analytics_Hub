#!/bin/bash

echo "========= UNIVERSAL INCREMENTAL INGESTION FOR ALL TFL LINES ========="

# PostgreSQL CONFIG
hostName="18.134.163.221"
dbName="testdb"
userName="admin"
password="admin123"

# Hive CONFIG
HIVE_DATABASE="batchprocessing_tfl_db"
HIVE_URL="jdbc:hive2://ip-172-31-14-3.eu-west-2.compute.internal:10000/${HIVE_DATABASE}"

# HDFS Base Directory
BASE_HDFS="/tmp/DE011025/TFL_Batch_processing/bronze"

# TFL Lines List
declare -a TFL_LINES=(
  "bakerloo"
  "central"
  "metropolitan"
  "northern"
  "piccadilly"
  "victoria"
)

# LOOP THROUGH ALL LINES
for LINE in "${TFL_LINES[@]}"; do
  echo ""
  echo "Processing line: ${LINE}"

  PG_TABLE="public.\"TFL_${LINE}_lines\""
  HIVE_TABLE="tfl_${LINE}_lines_raw"

  # dynamic HDFS folder for each run
  RUN_TS=$(date +"%Y%m%d_%H%M%S")
  HDFS_PATH="${BASE_HDFS}/TFL_${LINE}_lines/run_${RUN_TS}"

  echo "Postgres table : ${PG_TABLE}"
  echo "Hive table     : ${HIVE_TABLE}"
  echo "HDFS path      : ${HDFS_PATH}"
  echo ""

  # FETCH last api_fetch_time
  echo "Fetching last API fetch timestamp from Hive..."

  LAST_TS=$(beeline -u "$HIVE_URL" --silent=true --outputformat=csv2 -e "
      USE ${HIVE_DATABASE};
      SELECT MAX(api_fetch_time) FROM ${HIVE_TABLE};
  " 2>/dev/null | tail -1)

  echo "Last API fetch timestamp = ${LAST_TS}"

  # Build WHERE condition
  if [[ -z "$LAST_TS" || "$LAST_TS" == "NULL" ]]; then
      echo "No previous data → FULL LOAD"
      SQL_CONDITION="1=1"
  else
      echo "Incremental → api_fetch_time > '${LAST_TS}'"
      SQL_CONDITION="api_fetch_time > '${LAST_TS}'"
  fi

  echo "Running Sqoop import with filter: ${SQL_CONDITION}"

  # RUN SQOOP
  sqoop import \
    --connect jdbc:postgresql://${hostName}:5432/${dbName} \
    --username ${userName} \
    --password ${password} \
    --query "SELECT * FROM ${PG_TABLE} WHERE ${SQL_CONDITION} AND \$CONDITIONS" \
    --target-dir ${HDFS_PATH} \
    --m 1 \
    --as-textfile

  # CHECK STATUS
  if [[ $? -eq 0 ]]; then
      echo "SUCCESS → Sqoop import completed for ${LINE}"
      echo "Data stored in: ${HDFS_PATH}"
  else
      echo "ERROR → Sqoop import FAILED for ${LINE}"
      exit 1
  fi

done

echo "==================== ALL TFL LINES COMPLETED ===================="

