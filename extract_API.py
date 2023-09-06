import requests
import json
import os
from hdfs import InsecureClient  # Import the hdfs library

# Specify the specific path where you want to create the "weather_data" directory
weather_path = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data"

# Define the API URL
api_url = "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-05-01&end_date=2023-08-31&hourly=temperature_2m,relativehumidity_2m,dewpoint_2m,apparent_temperature,rain,pressure_msl,surface_pressure,vapor_pressure_deficit,soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_temperature_28_to_100cm,soil_temperature_100_to_255cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,soil_moisture_28_to_100cm,soil_moisture_100_to_255cm"

# Send the API request
response = requests.get(api_url)

# Check if the request was successful
if response.status_code == 200:
    # Create the "weather_data" directory in the specific path if it doesn't exist
    weather_data_directory = os.path.join(weather_path, "weather_data")
    if not os.path.exists(weather_data_directory):
        os.makedirs(weather_data_directory)

    # Define the file path for saving the JSON data
    file_path = os.path.join(weather_data_directory, "weather_data.json")

    # Save the JSON response to the local file
    with open(file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    print(f"JSON data saved locally to {file_path}")

    # Now, upload the local file to HDFS (Hadoop Distributed File System)
    hdfs_client = InsecureClient("localhost:9000", user="anna")
    
    # Define the HDFS path where you want to store the JSON data
    hdfs_path = "/anna/test1"
    
    # Upload the local file to HDFS
    hdfs_client.upload(hdfs_path, file_path)
    
    print(f"JSON data uploaded to HDFS at {hdfs_path}")
else:
    print(f"API request failed with status code: {response.status_code}")
