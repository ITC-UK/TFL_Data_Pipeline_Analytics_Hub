import time
import requests
from kafka import KafkaProducer
import json

api_list = {
    "piccadilly":   "https://api.tfl.gov.uk/Line/Piccadilly/Arrivals",
    "northern":     "https://api.tfl.gov.uk/Line/Northern/Arrivals",
    "central":      "https://api.tfl.gov.uk/Line/Central/Arrivals",
    "bakerloo":     "https://api.tfl.gov.uk/Line/Bakerloo/Arrivals",
    "metropolitan": "https://api.tfl.gov.uk/Line/Metropolitan/Arrivals",
    "victoria":     "https://api.tfl.gov.uk/Line/Victoria/Arrivals"
}

app_id  = "92293faa428041caad3dd647d39753a0"
app_key = "ba72936a3db54b4ba5792dc8f7acc043"

producer = KafkaProducer(
    bootstrap_servers=['ip-172-31-3-80.eu-west-2.compute.internal:9092'],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "ukde011025tfldata"

while True:
    print("Pulling TFL updates...")

    for line_name, api in api_list.items():
        url = "{}?app_id={}&app_key={}".format(api, app_id, app_key)
        data = requests.get(url).json()

        for event in data:
            event["line"] = line_name  # add metadata

            producer.send(topic, value=event)

        print("[SENT] {line_name} updates".format(line_name=line_name))

    producer.flush()
    print("Sleeping 30 seconds...\n")
    time.sleep(30)
