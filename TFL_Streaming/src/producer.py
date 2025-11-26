from pyspark.sql import SparkSession
import requests
from kafka import KafkaProducer
import json

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-21.0.7.0.6-2.el8.x86_64/bin/java"
os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + os.pathsep + os.environ["PATH"]



# Initialize SparkSession
spark = SparkSession.builder.appName("TrainArrival").getOrCreate()

# API URL
api_url = "https://api.tfl.gov.uk/Line/victoria/Arrivals?app_id=92293faa428041caad3dd647d39753a0&app_key=ba72936a3db54b4ba5792dc8f7acc043"

# Optional headers if needed (define accordingly)
headers = {
    # 'Authorization': 'Bearer <token>',  # example if needed
}

# Fetch API response
response = requests.get(api_url, headers=headers)
response.raise_for_status()
total = response.text

# Create DataFrame from JSON string
df_from_text = spark.read.json(spark.sparkContext.parallelize([total]))

# Select desired columns
message_df = df_from_text.select("id", "stationName", "lineName", "towards", "expectedArrival")

# Convert DataFrame rows to JSON strings (one per row)
message_json_list = message_df.toJSON().collect()
# Join all JSON strings with newline as in original code
message_json = "\n".join(message_json_list)

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['ip-172-31-3-80.eu-west-2.compute.internal:9092'],
    key_serializer=str.encode,
    value_serializer=str.encode
)

# Kafka topic and key
topic = "sylvester_test"
key = "1"

# Send message to Kafka
producer.send(topic, key=key, value=message_json)

# Flush and close the producer
producer.flush()
producer.close()
