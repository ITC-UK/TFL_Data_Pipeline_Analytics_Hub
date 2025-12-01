import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import yaml

# Load environment variables
load_dotenv()

# Load dev config
with open("config/dev.yaml", "r") as f:
    config = yaml.safe_load(f)

RAW_DIR = config["paths"]["raw_data"]

PG_USER = config["postgres"]["user"]
PG_PASSWORD = quote_plus(os.getenv("PG_PASSWORD"))
PG_HOST = config["postgres"]["host"]
PG_DB = config["postgres"]["database"]
PG_PORT = config["postgres"]["port"]

def get_engine():
    """Create and return a SQLAlchemy engine."""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        )
        print("DB connected successfully")
        return engine
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        raise

def load_csv_to_postgres():
    """Load CSV files from RAW_DIR into Postgres."""
    engine = get_engine()

    csv_files = [f for f in os.listdir(RAW_DIR) if f.endswith(".csv")]

    for file in csv_files:
        table_name = f"TFL_{file.replace('.csv','').lower()}_lines"
        file_path = os.path.join(RAW_DIR, file)

        # Wrap CSV read in try/except
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

        try:
            df.to_sql(table_name, engine, if_exists="append", index=False)
            print(f"{len(df)} rows appended to {table_name}")
        except Exception as e:
            print(f"Error inserting {file} into {table_name}: {e}")


if __name__ == "__main__":
    load_csv_to_postgres()
