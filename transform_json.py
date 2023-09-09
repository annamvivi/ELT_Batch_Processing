from hdfs import InsecureClient
import pandas as pd
import json
import datetime

# Create an HDFS client
hdfs_client = InsecureClient("http://localhost:9870", user="anna")

# Define the HDFS path to your JSON file
hdfs_input_path = "/user/anna/weather_data.json"

# Specify the output path
hdfs_output_path = "/user/anna/weather_data_modified.json"  

# Read the JSON data from HDFS
with hdfs_client.read(hdfs_input_path) as reader:
    # Load the JSON data into a Python dictionary
    json_data = json.load(reader)

# Define the keys you want to delete from the JSON data
keys_to_delete = ["latitude", "longitude", "generationtime_ms", "utc_offset_seconds", "timezone", "timezone_abbreviation", "elevation", "hourly_units"]

# Delete the specified keys from the JSON data
for key in keys_to_delete:
    json_data.pop(key, None)

# Convert the modified dictionary back to a JSON-formatted string with indentation
formatted_json = json.dumps(json_data, indent=4)

# Specify the file path in your local directory where you want to save the modified JSON
local_json_path = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/weather_data_modified.json"

# Write the formatted JSON data to the specified file in your local directory
with open(local_json_path, "w") as local_file:
    local_file.write(formatted_json)

print(f"Modified JSON data saved locally to {local_json_path}")

# # Write the formatted JSON data back to HDFS
# with hdfs_client.write(hdfs_output_path) as writer:
#     writer.write(formatted_json)

# print(f"Modified JSON data saved to HDFS at {hdfs_output_path}")

# Create an HDFS client
hdfs_client = InsecureClient("http://localhost:9870", user="anna")

# Define the HDFS path to your JSON file
hdfs_json_path = "/user/anna/weather_data_modified.json"

# Download the original JSON data from HDFS
with hdfs_client.read(hdfs_json_path) as reader:
    original_json_data = json.loads(reader.read())

# Access the "time" array from your JSON data
time_array = original_json_data["hourly"]["time"]

# Create lists to store the separated date and time components
dates = []
times = []

# Iterate through the datetime strings and separate date and time
for datetime_str in time_array:
    date_str, time_str = datetime_str.split('T')
    dates.append(date_str)
    times.append(time_str)

# Modify the original JSON data to include the separated date and time components
original_json_data["hourly"]["date"] = dates
original_json_data["hourly"]["time"] = times

# Serialize the modified JSON data to a JSON string
modified_json_string = json.dumps(original_json_data, indent=4)

# Print the new JSON string
print(modified_json_string)

# Specify the file path in your local directory where you want to save the modified JSON
local_json_path2 = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/weather_data_transformed_second.json"

# Write the formatted JSON data to the specified file in your local directory
with open(local_json_path2, "w") as local_file:
    local_file.write(modified_json_string)

print(f"Modified JSON data saved locally to {local_json_path2}")

# Read the JSON file into a Pandas DataFrame
#df = pd.read_json(local_json_path)

# Display the DataFrame
#print(df)

# Access the arrays from your original JSON data
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

# Create a list to store the transformed data
transformed_data = []

# Iterate through the arrays and create dictionaries
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

# Print the transformed data
for item in transformed_data:
    print(item)

# Serialize the modified JSON data to a JSON string
final_json_string = json.dumps(transformed_data, indent=4)

# Print the new JSON string
print(final_json_string)

# Specify the file path in your local directory where you want to save the modified JSON
local_json_path3 = "/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/weather_data_transformed_final.json"

# Write the formatted JSON data to the specified file in your local directory
with open(local_json_path3, "w") as local_file:
    local_file.write(final_json_string)

print(f"Modified JSON data saved locally to {local_json_path3}")

hdfs_json_final = "/user/anna/weather_data_final.json"

# Write the formatted JSON data back to HDFS
with hdfs_client.write(hdfs_json_final) as writer:
    writer.write(final_json_string)

print(f"Modified JSON data saved to HDFS at {hdfs_json_final}")
