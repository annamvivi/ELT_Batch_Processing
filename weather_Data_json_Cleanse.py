import os
import json
from hdfs import InsecureClient

class WeatherDataTransformer:
    def __init__(self, hdfs_base_url, hdfs_user):
        self.hdfs_client = InsecureClient(hdfs_base_url, user=hdfs_user)

    def load_json_from_hdfs(self, hdfs_input_path):
        with self.hdfs_client.read(hdfs_input_path) as reader:
            return json.load(reader)
        
    def load_json_from_local(self, local_input_path):
        with open(local_input_path, "r") as local_file:
            return json.load(local_file)

    def delete_keys_and_save_locally(self, json_data, keys_to_delete, local_path):
        for key in keys_to_delete:
            json_data.pop(key, None)

        formatted_json = json.dumps(json_data, indent=4)

        with open(local_path, "w") as local_file:
            local_file.write(formatted_json)

    def separate_datetime(self, original_json_data, local_path):
        time_array = original_json_data["hourly"]["time"]
        dates = []
        times = []

        for datetime_str in time_array:
            date_str, time_str = datetime_str.split('T')
            dates.append(date_str)
            times.append(time_str)

        original_json_data["hourly"]["date"] = dates
        original_json_data["hourly"]["time"] = times

        modified_json_string = json.dumps(original_json_data, indent=4)

        with open(local_path, "w") as local_file:
            local_file.write(modified_json_string)

    def transform_and_save(self, original_json_data, local_path):
        time_array = original_json_data["hourly"]["time"]
        temperature_array = original_json_data["hourly"]["temperature_2m"]
        date_array = original_json_data["hourly"]["date"]
        relativehumidity_array = original_json_data["hourly"]["relativehumidity_2m"]
        dewpoint_array = original_json_data["hourly"]["dewpoint_2m"]
        apparent_temperature_array = original_json_data["hourly"]["apparent_temperature"]
        rain_array = original_json_data["hourly"]["rain"]
        presure_array = original_json_data["hourly"]["pressure_msl"]
        surface_pressure_array = original_json_data["hourly"]["surface_pressure"]
        cloudcover_array = original_json_data["hourly"]["cloudcover"]
        evapotranspiration_array = original_json_data["hourly"]["et0_fao_evapotranspiration"]
        soil_temperature_0_to_7cm_array = original_json_data["hourly"]["soil_temperature_0_to_7cm"]
        soil_temperature_7_to_28cm_array = original_json_data["hourly"]["soil_temperature_7_to_28cm"]
        soil_temperature_28_to_100cm_array = original_json_data["hourly"]["soil_temperature_28_to_100cm"]
        soil_temperature_100_to_255cm_array = original_json_data["hourly"]["soil_temperature_100_to_255cm"]
        soil_moisture_0_to_7cm_array = original_json_data["hourly"]["soil_moisture_0_to_7cm"]
        soil_moisture_7_to_28cm_array = original_json_data["hourly"]["soil_moisture_7_to_28cm"]
        soil_moisture_28_to_100cm_array = original_json_data["hourly"]["soil_moisture_28_to_100cm"]
        soil_moisture_100_to_255cm_array = original_json_data["hourly"]["soil_moisture_100_to_255cm"]

        transformed_data = []

        for i in range(len(time_array)):
            data_point = {
                "date": date_array[i],
                "time": time_array[i],
                "temperature_2m": temperature_array[i],
                "relativehumidity_2m": relativehumidity_array[i],
                "dewpoint_2m": dewpoint_array[i],
                "apparent_temperature": apparent_temperature_array[i],
                "rain": rain_array[i],
                "pressure": presure_array[i],
                "surface_pressure": surface_pressure_array[i],
                "cloudcover": cloudcover_array[i],
                "evapotranspiration": evapotranspiration_array[i],
                "soil_temperature_0_to_7cm": soil_temperature_0_to_7cm_array[i],
                "soil_temperature_7_to_28cm": soil_temperature_7_to_28cm_array[i],
                "soil_temperature_28_to_100cm": soil_temperature_28_to_100cm_array[i],
                "soil_temperature_100_to_255cm": soil_temperature_100_to_255cm_array[i],
                "soil_moisture_0_to_7cm": soil_moisture_0_to_7cm_array[i],
                "soil_moisture_7_to_28cm": soil_moisture_7_to_28cm_array[i],
                "soil_moisture_28_to_100cm": soil_moisture_28_to_100cm_array[i],
                "soil_moisture_100_to_255cm": soil_moisture_100_to_255cm_array[i]
            }
            transformed_data.append(data_point)

        final_json_string = json.dumps(transformed_data, indent=4)

        with open(local_path, "w") as local_file:
            local_file.write(final_json_string)
