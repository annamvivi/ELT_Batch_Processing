import json
from kafka import KafkaConsumer

if __name__ == '__main__':
    # Kafka Consumer
    consumer = KafkaConsumer("weather-topic", bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

    for message in consumer:
        try:
            parsed_data = json.loads(message.value)
            # Process the parsed data here
            print(parsed_data)
        except json.JSONDecodeError as e:
            # Handle the JSON decoding error
            print(f"Error decoding JSON: {e}")
