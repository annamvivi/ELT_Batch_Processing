import json

# Specify the path to your original JSON file
json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/reformatted_weather_data.json'

# Specify the path for the new file to save the reformatted JSON
output_json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/reformatted_weather_data_fin.json'

# Read the original weather data from the JSON file
with open(json_file_path, 'r') as json_file:
    original_data = json.load(json_file)

# Initialize a dictionary to store the transformed data
transformed_data = {}

# Process the original data
for item in original_data:
    key = item["device"]
    value = item["value"]
    
    # If the key is "time," start a new entry in transformed_data
    if key == "time":
        current_timestamp = value
        transformed_data[current_timestamp] = {}
    else:
        # Add the value with the key to the current timestamp entry
        transformed_data[current_timestamp][key] = value

# Convert the transformed data into a list of dictionaries
formatted_data = [{"time": key, **values} for key, values in transformed_data.items()]

# Write the transformed data to a new JSON file
with open(output_json_file_path, "w") as output_file:
    json.dump(formatted_data, output_file, indent=4)

print(f"Transformation complete. Transformed data saved to {output_json_file_path}")
