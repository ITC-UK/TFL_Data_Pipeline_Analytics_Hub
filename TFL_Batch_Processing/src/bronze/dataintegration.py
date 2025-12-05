import os
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import yaml

# Load environment variables
load_dotenv()

# Get absolute project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config/dev.yaml")

# Load YAML config safely
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

RAW_DIR = os.path.join(BASE_DIR, config["paths"]["raw_data"])

PG_USER = config["postgres"]["user"]
PG_PASSWORD = quote_plus(os.getenv("PG_PASSWORD"))
PG_HOST = config["postgres"]["host"]
PG_DB = config["postgres"]["database"]
PG_PORT = config["postgres"]["port"]

INCREMENTAL_COL = "event_time"   # Change this if your timestamp column is different


def get_engine():
    """Create and return a SQLAlchemy engine."""
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    print("DB connected successfully")
    return engine


def get_last_timestamp(engine, table_name):
    """Return the max timestamp from the Postgres table."""
    query = text(f"SELECT MAX({INCREMENTAL_COL}) FROM {table_name}")
    try:
        result = engine.execute(query).fetchone()[0]
        return result
    except Exception:
        return None  # Table does not exist yet


def load_csv_incremental(engine, file, file_path):
    """Load ONLY new data into Postgres."""
    table_name = f"TFL_{file.replace('.csv','').lower()}_lines"

    # Read CSV
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file}: {e}")
        return

    # Ensure timestamp column exists
    if INCREMENTAL_COL not in df.columns:
        print(f"❌ ERROR: '{INCREMENTAL_COL}' column missing in {file}")
        return

    df[INCREMENTAL_COL] = pd.to_datetime(df[INCREMENTAL_COL], errors="coerce")

    last_ts = get_last_timestamp(engine, table_name)

    if last_ts:
        print(f"Last timestamp in DB for {table_name}: {last_ts}")
        df_new = df[df[INCREMENTAL_COL] > last_ts]
        print(f"Incremental rows found: {len(df_new)}")
    else:
        print(f"First load for {table_name}. Performing FULL load.")
        df_new = df

    if df_new.empty:
        print(f"No new rows to insert for {table_name}")
        return

    try:
        df_new.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"Inserted {len(df_new)} rows into {table_name}")
    except Exception as e:
        print(f"Error inserting rows: {e}")


def load_csv_to_postgres(mode="inc"):
    """Load CSV files using full or incremental mode."""
    engine = get_engine()

    csv_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]

    for file in csv_files:
        file_path = os.path.join(RAW_DIR, file)
        table_name = f"TFL_{file.replace('.csv','').lower()}_lines"

        print(f"\nProcessing file: {file}")

        if mode == "full":
            # Full Load - Replace table
            try:
                df = pd.read_csv(file_path)
                df.to_sql(table_name, engine, if_exists="replace", index=False)
                print(f"FULL LOAD completed: {len(df)} rows → {table_name}")
            except Exception as e:
                print(f"Error during full load: {e}")

        elif mode == "inc":
            # Incremental Load
            load_csv_incremental(engine, file, file_path)

        else:
            print("Invalid mode. Use 'full' or 'inc'.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        mode = "inc"  # Default mode = incremental

    load_csv_to_postgres(mode)
