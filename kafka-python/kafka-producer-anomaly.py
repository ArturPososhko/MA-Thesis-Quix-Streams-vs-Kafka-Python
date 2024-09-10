import logging
import sys
import os
import time
import json
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from datasource.TemperatureReadings import TemperatureEventGenerator

# Configure logging
log_file = "kafka_producer_anomaly.txt"
logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler(log_file, mode='w'),
    logging.StreamHandler(sys.stdout)
])

# Function for the Kafka-Python producer, which generates temperature events and sends them to the topic
def kafka_python_producer():
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    event_generator = TemperatureEventGenerator()

    while event := event_generator.generate_event():
        key = event["key"]
        value = event["value"]
        logging.info(f"Kafka-Python Producer: Producing event for MID {key}, {value}")
        producer.send('kafka-temperature-readings', key=key.encode('utf-8'), value=value)
        time.sleep(0.2) # Simulate real-time event production with a delay

    logging.info("Kafka-Python Producer: Stopping producing.")
    producer.flush()
    producer.close()

if __name__ == "__main__":
    kafka_python_producer()
