#!python3

import json
import time
import os

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

if __name__ == "__main__":
    # Specify the full path to the JSON file
    file_path = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/weather_data_transformed_final.json"

    # Check if the file exists at the specified path
    if not os.path.exists(file_path):
        print(f"File not found at {file_path}")
    else:
        # Read data from the JSON file
        with open(file_path, 'rb') as file:
            data = json.load(file)

        # Connect to the Kafka server
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                                value_serializer=json_serializer)

        # Push data to the Kafka server with topic "weather-topic"
        while True:
            for item in data:
                print(item)
                producer.send("weather-topic", item)
                #add a time delay here if needed
                time.sleep(10)

