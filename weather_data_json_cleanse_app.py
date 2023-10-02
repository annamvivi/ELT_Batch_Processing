import os
from weather_Data_json_Cleanse import WeatherDataTransformer

if __name__ == "__main__":
    hdfs_base_url = "http://localhost:9870"
    hdfs_user = "anna"
    hdfs_input_path = "/user/anna/weather_data.json"
    hdfs_output_path = "/user/anna/weather_data_modified.json"
    base_directory = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/"

    # Local file paths
    local_input_path = base_directory
    local_path1 = os.path.join(local_input_path, "weather_data.json")
    local_path2 = os.path.join(local_input_path, "weather_data_modified.json")
    local_path3 = os.path.join(local_input_path, "weather_data_transformed_final.json")

    transformer = WeatherDataTransformer(hdfs_base_url, hdfs_user)

    # Load data from HDFS
    json_data = transformer.load_json_from_hdfs(hdfs_input_path)

    # Cleanse data and save locally
    transformer.delete_keys_and_save_locally(json_data, ["latitude", "longitude", "generationtime_ms", "utc_offset_seconds", "timezone", "timezone_abbreviation", "elevation", "hourly_units"], local_path2)

    # Perform transformation locally
    original_json_data = transformer.load_json_from_hdfs(local_path2)
    transformer.separate_datetime(original_json_data, local_path3)

    # Upload the final result to HDFS
    transformer.hdfs_client.upload(hdfs_output_path, local_path3)
