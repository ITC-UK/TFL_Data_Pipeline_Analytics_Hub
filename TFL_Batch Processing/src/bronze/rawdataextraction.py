import os
import time
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import yaml

# Load environment variables
load_dotenv()

# Load dev config
with open("config/dev.yaml", "r") as f:
    config = yaml.safe_load(f)

RAW_DIR = config["paths"]["raw_data"]
os.makedirs(RAW_DIR, exist_ok=True)
print(f"Saving files to: {RAW_DIR}")

# TfL API URLs for all lines
urls = {
    "Piccadilly": "https://api.tfl.gov.uk/Line/Piccadilly/Arrivals",
    "Northern": "https://api.tfl.gov.uk/Line/Northern/Arrivals",
    "Central": "https://api.tfl.gov.uk/Line/Central/Arrivals",
    "Bakerloo": "https://api.tfl.gov.uk/Line/Bakerloo/Arrivals",
    "Metropolitan": "https://api.tfl.gov.uk/Line/Metropolitan/Arrivals",
    "Victoria": "https://api.tfl.gov.uk/Line/Victoria/Arrivals"
}

while True:
    for line_name, url in urls.items():
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                print(f"No data returned for {line_name}")
                continue

            df_new = pd.json_normalize(data)
            df_new["api_fetch_time"] = datetime.utcnow()

            file_path = os.path.join(RAW_DIR, f"{line_name}.csv")

            if os.path.exists(file_path):
                df_old = pd.read_csv(file_path)
                df_combined = pd.concat([df_old, df_new], ignore_index=True)
                df_combined.drop_duplicates(subset=["id"], inplace=True)
                df_combined.to_csv(file_path, index=False)
                print(f"Updated {file_path} → {len(df_combined)} rows")
            else:
                df_new.to_csv(file_path, index=False)
                print(f"Created {file_path} → {len(df_new)} rows")

        except Exception as e:
            print(f"Error fetching {line_name}: {e}")

    print("\nWaiting 60 seconds before next fetch...\n")
    time.sleep(60)
