import requests
import pandas as pd
import time
import os
from datetime import datetime

# TfL API URLs
urls = {
    "Piccadilly": "https://api.tfl.gov.uk/Line/Piccadilly/Arrivals",
    "Northern": "https://api.tfl.gov.uk/Line/Northern/Arrivals",
    "Central": "https://api.tfl.gov.uk/Line/Central/Arrivals",
    "Bakerloo": "https://api.tfl.gov.uk/Line/Bakerloo/Arrivals",
    "Metropolitan": "https://api.tfl.gov.uk/Line/Metropolitan/Arrivals",
    "Victoria": "https://api.tfl.gov.uk/Line/Victoria/Arrivals"
}

# Path of this file → .../TFL_Batch Processing/src/bronze/rawdataextraction.py
CURRENT_FILE = os.path.abspath(__file__)

# Move up to: .../TFL_Batch Processing/src/
SRC_FOLDER = os.path.dirname(os.path.dirname(CURRENT_FILE))

# Move up again to: .../TFL_Batch Processing/
BASE_DIR = os.path.dirname(SRC_FOLDER)

# Now build the raw folder path
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

print(f"Saving files to: {RAW_DIR}")

while True:
    for name, url in urls.items():
        print(f"Fetching {name} line...")

        try:
            response = requests.get(url, timeout=30)
            data = response.json()

            if not data:
                print(f"No data returned for {name}")
                continue

            # Convert JSON → DataFrame (all columns included)
            df_new = pd.json_normalize(data)

            # Add ingestion timestamp
            df_new["api_fetch_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            file_path = os.path.join(RAW_DIR, f"{name}.csv")

            # Append only NEW data
            if os.path.exists(file_path):
                df_old = pd.read_csv(file_path)

                df_combined = pd.concat([df_old, df_new], ignore_index=True)

                # Remove duplicates based on ID
                df_combined.drop_duplicates(subset=["id"], inplace=True)

                df_combined.to_csv(file_path, index=False)

                print(f"Updated {file_path} → {len(df_combined)} rows")

            else:
                df_new.to_csv(file_path, index=False)
                print(f"Created {file_path} → {len(df_new)} rows")

        except Exception as e:
            print(f"Error fetching {name}: {e}")

    print("\n Waiting 60 seconds before next fetch...\n")
    time.sleep(60)
