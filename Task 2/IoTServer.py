import json
import random
import time
from kafka import KafkaProducer

# Kafka broker address
kafka_bootstrap_servers = '10.1.7.194:9092'

# Kafka topic
kafka_topic = 'test_2153599'

# Generate data
def generate_random_data():
    date = time.strftime("%m/%d/%y")                            # Take current date
    humidity = "{:.1f}".format(random.uniform(20.0, 80.0))      # Range: 20.0 => 80.0
    temperature = "{:.1f}".format(random.uniform(18.0, 30.0))   # Range: 18.0 => 30.0
    current_time = time.strftime("%H:%M:%S")                    # Take the current time

    return {
        "date": date,
        "humidity": humidity,
        "temperature": temperature,
        "time": current_time
    }

def send_message(producer, topic, message):
    producer.send(topic, json.dumps(message).encode("utf-8"))
    print(f"Sent message: {message}")

def main():
    # Create Kafka producer
    producer = KafkaProducer()

    try:
        while True:
            # Generate random IoT data
            iot_data = generate_random_data()

            # Send message to Kafka topic
            send_message(producer, kafka_topic, iot_data)

            # Sleep for a random interval (simulating periodic data)
            time.sleep(random.uniform(1, 5))

    except KeyboardInterrupt:
        print("Exiting IoT Server.")

if __name__ == "__main__":
    main()
