import os
import json
import requests
from hdfs import InsecureClient

class WeatherDataDownloader:
    def __init__(self, weather_path, hdfs_base_url, hdfs_user):
        self.weather_path = weather_path
        self.hdfs_base_url = hdfs_base_url
        self.hdfs_user = hdfs_user

    def download_weather_data(self, api_url):
        # Send the API request
        response = requests.get(api_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Create the "weather_data" directory in the specific path if it doesn't exist
            weather_data_directory = os.path.join(self.weather_path, "weather_data")
            if not os.path.exists(weather_data_directory):
                os.makedirs(weather_data_directory)

            # Define the file path for saving the JSON data
            file_path = os.path.join(weather_data_directory, "weather_data.json")

            # Save the JSON response to the local file
            with open(file_path, "w") as json_file:
                json.dump(response.json(), json_file, indent=4)

            print(f"JSON data saved locally to {file_path}")
        else:
            print(f"API request failed with status code: {response.status_code}")

    def upload_weather_data_to_hdfs(self, local_file_path, hdfs_path):
        # Now, upload the local file to HDFS (Hadoop Distributed File System)
        hdfs_client = InsecureClient(self.hdfs_base_url, user=self.hdfs_user)

        # Upload the local file to HDFS
        hdfs_client.upload(hdfs_path, local_file_path)

        print(f"JSON data uploaded to HDFS at {hdfs_path}")

