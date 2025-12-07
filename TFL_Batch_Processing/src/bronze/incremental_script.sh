#!/bin/bash

echo "========= UNIVERSAL INCREMENTAL INGESTION (RAW OVERWRITE MODE) ========="

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
  "central"
  "metropolitan"
  "northern"
  "piccadilly"
  "victoria"
)

# ------------------------------------------------------------
# RETRY FUNCTION FOR SQOOP
# ------------------------------------------------------------
run_with_retry() {
    local cmd="$1"
    local attempts=3
    local delay=30

    for ((i=1; i<=attempts; i++)); do
        echo "Attempt $i of $attempts..."
        eval "$cmd"

        if [[ $? -eq 0 ]]; then
            echo "✔ Command succeeded on attempt $i"
            return 0
        fi

        echo " Command failed on attempt $i"
        if [[ $i -lt $attempts ]]; then
            echo "⏳ Retrying in ${delay} seconds..."
            sleep $delay
        fi
    done

    echo "All retry attempts failed!"
    return 1
}

# ------------------------------------------------------------
# MAIN INGESTION LOOP
# ------------------------------------------------------------
for LINE in "${TFL_LINES[@]}"; do
  echo ""
  echo "============ PROCESSING LINE: ${LINE} ============"

  PG_TABLE="public.\"TFL_${LINE}_lines\""
  HIVE_TABLE="tfl_${LINE}_lines_raw"

  # Dynamic output folder
  RUN_TS=$(date +"%Y%m%d_%H%M%S")
  HDFS_PATH="${BASE_HDFS}/TFL_${LINE}_lines/run_${RUN_TS}"

  echo "Postgres table : ${PG_TABLE}"
  echo "Hive table     : ${HIVE_TABLE}"
  echo "HDFS path      : ${HDFS_PATH}"
  echo ""

  # ---------------- FETCH LAST TIMESTAMP FROM HIVE ----------------
  echo "Fetching last API timestamp from Hive..."
  LAST_TS=$(beeline -u "$HIVE_URL" --silent=true --outputformat=csv2 -e "
        USE ${HIVE_DATABASE};
        SELECT MAX(api_fetch_time) FROM ${HIVE_TABLE};
    " 2>/dev/null | tail -1)

  echo "Last API timestamp = ${LAST_TS}"

  # ---------------- DETERMINE FULL OR INCREMENTAL ----------------
  if [[ -z "$LAST_TS" || "$LAST_TS" == "NULL" ]]; then
      echo "No previous data found → FULL LOAD"
      SQL_CONDITION="1=1"
  else
      echo "Incremental load → api_fetch_time > '${LAST_TS}'"
      SQL_CONDITION="api_fetch_time > '${LAST_TS}'"
  fi

  # ---------------- CLEAN OLD HDFS DIR ----------------
  echo "Cleaning HDFS output path..."
  hdfs dfs -rm -r -f "${HDFS_PATH}" >/dev/null 2>&1

  # ---------------- RUN SQOOP WITH RETRY ----------------
  echo "Running Sqoop import with retry..."

  SQOOP_CMD="sqoop import \
      --connect jdbc:postgresql://${hostName}:5432/${dbName} \
      --username ${userName} \
      --password ${password} \
      --query \"SELECT * FROM ${PG_TABLE} WHERE ${SQL_CONDITION} AND \\\$CONDITIONS\" \
      --target-dir ${HDFS_PATH} \
      --m 1 \
      --as-textfile"

  run_with_retry "$SQOOP_CMD"
  STATUS=$?

  # ---------------- CHECK STATUS ----------------
  if [[ $STATUS -eq 0 ]]; then
      echo "SUCCESS → Sqoop import completed for ${LINE}"
      echo "   Data stored in: ${HDFS_PATH}"
  else
      echo "FAILED → Sqoop import failed after retries for ${LINE}"
      exit 1
  fi

done

echo "==================== ALL TFL LINES COMPLETED SUCCESSFULLY ===================="
