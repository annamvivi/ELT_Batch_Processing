import json

# Specify the path to your original JSON file
json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/weather_data.json'

# Specify the path for the new file to save the reformatted JSON
reformatted_json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/reformatted_weather_data.json'

# Function to restructure the JSON
def restructure_weather_data(data):
    restructured_data = []
    for key, value in data.items():
        if isinstance(value, dict):
            restructured_data.append({"device": key, "value": restructure_weather_data(value)})
        elif isinstance(value, list):
            device_values = [{"device": key, "value": val} for val in value]
            restructured_data.extend(device_values)
        else:
            restructured_data.append({"device": key, "value": value})
    return restructured_data

# Read the original weather data from the JSON file
with open(json_file_path, 'r') as json_file:
    original_data = json.load(json_file)

# Restructure the data
restructured_json = restructure_weather_data(original_data)

# Encapsulate the restructured data within a list with 'device' and 'value' keys
formatted_data = [{"device": item["device"], "value": item["value"]} for item in restructured_json]

# Convert to JSON string
reformatted_json_string = json.dumps(formatted_data)

# Save the reformatted JSON to the new file
with open(reformatted_json_file_path, 'w') as output_json_file:
    output_json_file.write(reformatted_json_string)

print(f"Reformatted JSON saved to {reformatted_json_file_path}")

# Specify the path for the new file to save the transformed JSON
transformed_json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/transformed_weather_data.json'

# Initialize an empty dictionary to store the transformed data
transformed_data = {}

# Read the original JSON data from the reformatted file
with open(reformatted_json_file_path, 'r') as json_file:
    original_data = json.load(json_file)

#Process the original data
for item in original_data:
    device = item["device"]
    value = item["value"]
    if device not in transformed_data:
        transformed_data[device] = value

# Process the original data



# for item in original_data:
#     device = item["device"]
#     value = item["value"]
    
#     # If the key is "time," start a new entry in transformed_data
#     if device not in transformed_data:
#         transformed_data[device] = value
#     else:
#         # Add the value with the key to the current timestamp entry
#         transformed_data[current_timestamp][device] = value

# # Convert the transformed data into a list of dictionaries
# formatted_data = [{"time": key, **values} for key, values in transformed_data.items()]


# Write the transformed data to a new JSON file
with open(transformed_json_file_path, "w") as output_file:
    json.dump(transformed_data, output_file)

print(f"Transformation complete. Transformed data saved to {transformed_json_file_path}")



