import os
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from collections import deque
from statistics import mean

# Kafka settings
broker_address = os.environ.get("BROKER_ADDRESS", "localhost:9092")
consumer_group = "kafka-temperature-alerter"
temperature_readings_topic = "kafka-temperature-readings"
alerts_topic = "kafka-alerts"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    temperature_readings_topic,
    bootstrap_servers=broker_address,
    group_id=consumer_group,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Initialize Kafka producer for sending alerts
producer = KafkaProducer(
    bootstrap_servers=broker_address,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Window parameters
window_duration_ms = 5000
window_step_ms = 1000

# State for windowing (stores readings for each MID)
windows = {}
last_emit_time = time.time()

# Function to process each temperature reading
def process_temperature_reading(message):
    global last_emit_time

    current_time = int(time.time() * 1000)
    machine_id = message["MID"]
    
    # Initialize the deque for this MID if not already done
    if machine_id not in windows:
        windows[machine_id] = deque()

    # Append the temperature reading with the current timestamp to the window
    windows[machine_id].append((message["Temperature_C"], current_time))

    # Remove old messages outside of the window duration
    while windows[machine_id] and current_time - windows[machine_id][0][1] > window_duration_ms:
        windows[machine_id].popleft()

    # Emit window result every step_ms
    if current_time - last_emit_time >= window_step_ms:
        for mid, window in windows.items():
            temperatures = [temp for temp, _ in window]
            if temperatures:
                avg_temperature = mean(temperatures)
                avg_temperature = round(avg_temperature, 2)

                # Check if an alert should be sent
                if should_alert(avg_temperature, mid, message["Timestamp"], message.get("headers", {})):
                    alert = {
                        "Machine_ID": mid,
                        "Average_Temperature": avg_temperature,
                        "Timestamp": current_time
                    }
                    producer.send(alerts_topic, value=alert)
                    
        last_emit_time = current_time

# Function to check if an alert should be sent
def should_alert(window_value: float, key, timestamp, headers) -> bool:
    if window_value >= 90:
        print(f"Kafka-Python Consumer: Alerting for MID {key}: Average Temperature {window_value}")
        return True
    return False

# Process the incoming temperature data, apply windowing, and filter for alerts
try:
    for message in consumer:
        value = message.value
        value["MID"] = int(message.key.decode('utf-8'))
        process_temperature_reading(value)
finally:
    consumer.close()
    producer.close()

if __name__ == "__main__":
    print("Kafka Temperature Alerter is running...")