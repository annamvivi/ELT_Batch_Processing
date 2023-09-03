import requests
import json
import os

# Specify the specific path where you want to create the "weather_data" directory
weather_path = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data"

# Define the API URL
api_url = "https://archive-api.open-meteo.com/v1/archive?latitude=52.52&longitude=13.41&start_date=2023-07-01&end_date=2023-08-28&hourly=temperature_2m,relativehumidity_2m,dewpoint_2m,apparent_temperature,rain&daily=weathercode,temperature_2m_max,temperature_2m_min,temperature_2m_mean,apparent_temperature_max,apparent_temperature_min,apparent_temperature_mean,rain_sum,windspeed_10m_max,windgusts_10m_max,winddirection_10m_dominant&timezone=Asia%2FSingapore"

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

    # Save the JSON response to the file
    with open(file_path, "w") as json_file:
        json.dump(response.json(), json_file, indent=4)

    print(f"JSON data saved to {file_path}")
else:
    print(f"API request failed with status code: {response.status_code}")
