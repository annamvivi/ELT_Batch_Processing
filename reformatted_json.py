import json

# Specify the path to your original JSON file
json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/weather_data.json'

# Specify the path for the new file to save the reformatted JSON
output_json_file_path = '/mnt/c/linux/ETL_Project/Python_ETL_Adv_Works/data/weather_data/reformatted_weather_data.json'

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
with open(output_json_file_path, 'w') as output_json_file:
    output_json_file.write(reformatted_json_string)

# Print a message indicating that the new file has been created
print(f"Reformatted JSON saved to {output_json_file_path}")
