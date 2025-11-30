#!/usr/bin/env python3.8
import os
import sys
import argparse
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from dotenv import load_dotenv  

def load_environment(env_path=None):
    load_dotenv(dotenv_path=env_path)

def create_db_engine():
    user = os.getenv("DB_USERNAME")
    password = quote_plus(os.getenv("DB_PASSWORD", ""))
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    if not all([user, password, host, port, db]):
        print("ERROR: Missing database environment variables.")
        sys.exit(1)

    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    try:
        engine = create_engine(conn_str, pool_pre_ping=True)
        print("PostgreSQL engine created successfully.")
        return engine
    except SQLAlchemyError as e:
        print(f"Engine creation failed: {e}")
        sys.exit(1)

def load_csv(line_name):
    base_path = os.getenv("RAW_PATH", "../../data/raw")
    csv_path = f"{base_path}/{line_name}.csv"

    if not os.path.exists(csv_path):
        print(f"ERROR: File not found: {csv_path}")
        return None

    df = pd.read_csv(csv_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df


# FULL LOAD
def full_load(engine, line):
    table = f"TFL_{line.lower()}_lines"

    df = load_csv(line)
    if df is None:
        return

    df.to_sql(table, engine, if_exists="replace", index=False)
    print(f"FULL LOAD: {len(df)} rows loaded → {table}")


# INCREMENTAL LOAD
def incremental_load(engine, line):
    table = f"TFL_{line.lower()}_lines"

    df = load_csv(line)
    if df is None:
        return

    with engine.connect() as conn:
        query = text(f'SELECT MAX("timestamp") FROM "{table}"')
        result = conn.execute(query)
        max_ts = result.scalar() or datetime(1970, 1, 1)

    print(f"{line}: Last timestamp in DB = {max_ts}")

    df_new = df[df["timestamp"] > max_ts].sort_values("timestamp")

    if df_new.empty:
        print(f"{line}: No new rows.")
        return

    df_new.to_sql(table, engine, if_exists="append", index=False)
    print(f"{line}: {len(df_new)} new rows inserted.")

# RUN FOR ALL LINES
TFL_LINES = ["Bakerloo", "Central", "Metropolitan", "Northern", "Piccadilly", "Victoria"]


def run_full(engine):
    print("\n STARTING FULL LOAD FOR ALL TFL LINES...\n")
    for line in TFL_LINES:
        full_load(engine, line)


def run_incremental(engine):
    print("\n STARTING INCREMENTAL LOAD FOR ALL TFL LINES...\n")
    for line in TFL_LINES:
        incremental_load(engine, line)

#  ARG PARSER
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["full", "inc"], help="ETL mode")
    return parser.parse_args()

# MAIN FUNCTION
def main():
    args = parse_args()

    load_environment(os.getenv("ENV_FILE"))
    engine = create_db_engine()

    try:
        if args.mode == "full":
            run_full(engine)
        if args.mode == "inc":
            run_incremental(engine)

    except Exception as e:
        print(" ETL FAILED")
        print(str(e))
        sys.exit(1)
    finally:
        engine.dispose()
        print("\n Postgres connection closed.")


if __name__ == "__main__":
    main()