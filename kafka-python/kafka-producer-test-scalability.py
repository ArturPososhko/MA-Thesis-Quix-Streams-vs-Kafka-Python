import os
import sys
import time
import json
import psutil
import numpy as np
import logging
from kafka import KafkaProducer
from threading import Thread
from matplotlib import pyplot as plt

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from datasource.TemperatureReadings import TemperatureEventGenerator

# Configurable parameters for the scalability test
initial_event_rate = 1  # Starting number of events per second
event_rate_increment = 10000  # Increment the event rate by this value
max_event_rate = 100001  # Maximum number of events per second for scalability test
increment_duration = 5  # Duration in seconds before increasing the event rate

# Global variables for producer metrics
messages_sent = 0
cpu_usage = []
cpu_per_core_usage = []
memory_usage = []
throughput_data = []
throughput_time = []
cpu_time = []
message_window_start = None
first_message_time = None

# Configure logging
log_file = "kafka_producer_scalability_logs.txt"
logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler(log_file, mode='w'),
    logging.StreamHandler(sys.stdout)
])

# Function to print the system's CPU and memory capacity
def print_system_capacity():
    cpu_count = psutil.cpu_count(logical=True)
    total_memory = psutil.virtual_memory().total / (1024 * 1024)
    logging.info(f"CPU Count: {cpu_count}")
    logging.info(f"Total Memory: {total_memory:.2f} MB")

# Function to monitor resources
def monitor_resources():
    global cpu_usage, memory_usage, cpu_per_core_usage, cpu_time, first_message_time
    cpu_per_core_count = psutil.cpu_count(logical=True)

    if len(cpu_per_core_usage) == 0:
        cpu_per_core_usage = [[] for _ in range(cpu_per_core_count)]

    # Wait until the producer starts sending messages to start logging CPU and memory usage
    while first_message_time is None:
        time.sleep(0.1)

    while True:
        total_cpu = psutil.cpu_percent(interval=1)
        per_core_cpu = psutil.cpu_percent(interval=0, percpu=True)
        cpu_usage.append(total_cpu)
        memory_usage.append(psutil.virtual_memory().percent)
        cpu_time.append(time.time() - first_message_time)

        # Track per-core CPU usage
        for i in range(cpu_per_core_count):
            cpu_per_core_usage[i].append(per_core_cpu[i])

        time.sleep(1)

# Kafka-Python Producer
def kafka_python_producer():
    global messages_sent, message_window_start, first_message_time, throughput_data, throughput_time

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    first_message_time = time.time()
    message_window_start = first_message_time

    current_event_rate = initial_event_rate
    temperature_event_generator = TemperatureEventGenerator(max_events_per_second=current_event_rate)

    start_time = time.time()

    # Produce messages while gradually increasing the event rate
    while current_event_rate <= max_event_rate:
        iteration_start_time = time.time()
        while time.time() - iteration_start_time < increment_duration:
            event = temperature_event_generator.generate_limited_event()
            producer.send('kafka-scalability-test', key=event["key"].encode('utf-8'), value=event["value"])
            messages_sent += 1

            # Log producer stats every second
            if time.time() - message_window_start >= 1:
                throughput_data.append(messages_sent)
                throughput_time.append(time.time() - first_message_time)
                logging.info(f"Produced {messages_sent} messages in the last second")
                messages_sent = 0
                message_window_start = time.time()

        # Increase the event rate after every increment_duration
        current_event_rate += event_rate_increment
        temperature_event_generator = TemperatureEventGenerator(max_events_per_second=current_event_rate)
        logging.info(f"Increasing event rate to {current_event_rate} messages per second")

    producer.flush()  # Ensure all messages are sent before stopping
    producer.close()

    logging.info("Kafka-Python producer scalability test stopped.")

# Function to print producer metrics and generate graphs
def print_producer_metrics():
    avg_cpu = np.mean(cpu_usage)
    avg_memory = np.mean(memory_usage)
    avg_output = np.mean(throughput_data)

    logging.info(f"CPU Usage (%) - Avg: {avg_cpu:.2f}")
    logging.info(f"Memory Usage (%) - Avg: {avg_memory:.2f}")
    logging.info(f"Message Output (msg/sec) - Avg: {avg_output:.2f}")

    # Calculate per-core CPU usage averages
    if len(cpu_per_core_usage) > 0:
        per_core_avg = [np.mean(core) for core in cpu_per_core_usage]
        logging.info("Average CPU usage per core:")
        for i, avg in enumerate(per_core_avg):
            logging.info(f"Core {i}: {avg:.2f}%")

    # Generate performance graphs
    plt.figure(figsize=(12, 8))

    # Plot message output rate
    plt.subplot(3, 1, 1)
    plt.plot(throughput_time, throughput_data)
    plt.title("Message Output (messages/sec)")
    plt.xlabel("Time (s)")
    plt.ylabel("Messages per Second")

    # Plot total CPU usage
    plt.subplot(3, 1, 2)
    plt.plot(cpu_time, cpu_usage)
    plt.title("CPU Usage (%)")
    plt.xlabel("Time (s)")
    plt.ylabel("CPU Usage (%)")

    # Plot memory usage
    plt.subplot(3, 1, 3)
    plt.plot(cpu_time, memory_usage)
    plt.title("Memory Usage (%)")
    plt.xlabel("Time (s)")
    plt.ylabel("Memory Usage (%)")

    # Save and display the plot
    plt.tight_layout()
    plt.savefig("kafka_producer_scalability_metrics.png")
    plt.show()

if __name__ == "__main__":
    print_system_capacity()

    # Start resource monitoring in a separate thread
    resource_monitor_thread = Thread(target=monitor_resources)
    resource_monitor_thread.daemon = True  # Runs in the background
    resource_monitor_thread.start()

    # Start the producer run
    kafka_python_producer()

    # Print and plot the producer metrics
    print_producer_metrics()