import os
from weather_data_downloader import WeatherDataDownloader

if __name__ == "__main__":
    weather_path = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data"
    hdfs_base_url = "http://localhost:9870"
    hdfs_user = "anna"
    api_url = "https://archive-api.open-meteo.com/v1/archive?latitude=-5&longitude=120&start_date=2023-07-01&end_date=2023-08-31&hourly=temperature_2m,relativehumidity_2m,dewpoint_2m,apparent_temperature,rain,pressure_msl,surface_pressure,cloudcover,et0_fao_evapotranspiration,vapor_pressure_deficit,soil_temperature_0_to_7cm,soil_temperature_7_to_28cm,soil_temperature_28_to_100cm,soil_temperature_100_to_255cm,soil_moisture_0_to_7cm,soil_moisture_7_to_28cm,soil_moisture_28_to_100cm,soil_moisture_100_to_255cm&timezone=Asia%2FSingapore"
    hdfs_path = "/user/anna"

    weather_downloader = WeatherDataDownloader(weather_path, hdfs_base_url, hdfs_user)
    
    # Download weather data
    weather_downloader.download_weather_data(api_url)

    # Define the local file path
    local_file_path = os.path.join(weather_path, "weather_data", "weather_data.json")

    # Upload weather data to HDFS
    weather_downloader.upload_weather_data_to_hdfs(local_file_path, hdfs_path)
