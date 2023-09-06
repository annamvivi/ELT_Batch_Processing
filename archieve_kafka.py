import os
import json
from kafka import KafkaConsumer

# Specify the directory where you want to store the archived data
archive_directory = "/path/to/your/directory/"

if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer("weather-topic", bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

    for message in consumer:
        try:
            parsed_data = json.loads(message.value)
            # Process the parsed data here
            process_data(parsed_data)
        except json.JSONDecodeError as e:
            # Handle the JSON decoding error
            print(f"Error decoding JSON: {e}")

def process_data(data):
    # Implement your data processing logic here
    print("Processing data:", data)
    
    # Example: Archive old data
    archive_old_data(data)
    
def archive_old_data(data):
    # Construct the full path to the archive file in the specified directory
    archive_file_path = os.path.join(archive_directory, "archived_data.txt")
    
    # Implement logic to archive old data in the specified directory
    print("Archiving old data:", data)
    
    # Example: Store old data in the specified directory
    with open(archive_file_path, "a") as file:
        file.write(json.dumps(data) + "\n")
