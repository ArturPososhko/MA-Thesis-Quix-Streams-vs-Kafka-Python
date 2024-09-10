import os
import sys
import time
import json
import psutil
import numpy as np
import logging
from matplotlib import pyplot as plt
from kafka import KafkaConsumer
from threading import Thread

# Configurable duration for the consumer
uptime_duration = 54  # Uptime of the consumer in seconds

# Global variables for consumer metrics
messages_consumed = 0
latency_stats = []
cpu_usage = []
cpu_per_core_usage = []
memory_usage = []
throughput_data = []
throughput_time = []
cpu_time = []
message_window_start = None
first_message_time = None

# Configure logging
log_file = "kafka_consumer_scalability_logs.txt"
logging.basicConfig(level=logging.INFO, handlers=[
    logging.FileHandler(log_file, mode='w'),
    logging.StreamHandler(sys.stdout)
])

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

    # Wait until the first message is consumed to start logging CPU and memory usage
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

# Kafka-Python Consumer
def kafka_python_consumer():
    global messages_consumed, latency_stats, message_window_start, first_message_time, throughput_data, throughput_time

    logging.info("Starting Kafka-Python consumer...")

    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        'kafka-scalability-test',
        bootstrap_servers=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
        auto_offset_reset='latest',
        group_id='kafka-scalability-consumer',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    for message in consumer:
        produced_timestamp = message.value["Timestamp"]
        consumed_timestamp = time.time_ns()

        # Track the first message
        if first_message_time is None:
            first_message_time = time.time()
            logging.info("First message received. Timer started.")
            message_window_start = first_message_time

        # Calculate latency
        latency = (consumed_timestamp - produced_timestamp) / 1_000_000
        latency_stats.append(latency)

        messages_consumed += 1

        # Log consumption stats every second
        if time.time() - message_window_start >= 1:
            throughput_data.append(messages_consumed)
            throughput_time.append(time.time() - first_message_time)
            logging.info(f"Consumed {messages_consumed} messages in the last second")
            messages_consumed = 0
            message_window_start = time.time()

        # Stop the consumer after the specified run duration
        if time.time() - first_message_time >= uptime_duration:
            logging.info(f"{uptime_duration} seconds have passed since the first message. Stopping the consumer.")
            break

    consumer.close()

# Function to print consumer metrics and generate graphs
def print_consumer_metrics():
    avg_cpu = np.mean(cpu_usage)
    avg_memory = np.mean(memory_usage)
    avg_latency = np.mean(latency_stats)
    avg_throughput = np.mean(throughput_data)

    logging.info(f"CPU Usage (%) - Avg: {avg_cpu:.2f}")
    logging.info(f"Memory Usage (%) - Avg: {avg_memory:.2f}")
    logging.info(f"Latency (ms) - Avg: {avg_latency:.2f}")
    logging.info(f"Throughput (messages/sec) - Avg: {avg_throughput:.2f}")

    if len(cpu_per_core_usage) > 0:
        per_core_avg = [np.mean(core) for core in cpu_per_core_usage]
        logging.info("Average CPU usage per core:")
        for i, avg in enumerate(per_core_avg):
            logging.info(f"Core {i}: {avg:.2f}%")

    plt.figure(figsize=(12, 8))

    # Plot latency
    plt.subplot(3, 1, 1)
    plt.plot(latency_stats)
    plt.title("Latency (ms)")
    plt.xlabel("Message Number")
    plt.ylabel("Latency (ms)")

    # Plot throughput
    plt.subplot(3, 1, 2)
    plt.plot(throughput_time, throughput_data)
    plt.title("Throughput (messages/sec)")
    plt.xlabel("Time (s)")
    plt.ylabel("Messages per Second")

    # Plot CPU and Memory usage
    plt.subplot(3, 1, 3)
    plt.plot(cpu_time, cpu_usage, label="CPU Usage (%)")
    plt.plot(cpu_time, memory_usage, label="Memory Usage (%)")
    plt.title("Resource Usage")
    plt.xlabel("Time (s)")
    plt.ylabel("Usage (%)")
    plt.legend()

    # Save and display the plot
    plt.tight_layout()
    plt.savefig("kafka_consumer_scalability_metrics.png")
    plt.show()

if __name__ == "__main__":
    print_system_capacity()

    # Start resource monitoring in a separate thread
    resource_monitor_thread = Thread(target=monitor_resources)
    resource_monitor_thread.daemon = True
    resource_monitor_thread.start()

    # Start the consumer run
    kafka_python_consumer()

    # Print and plot the consumer metrics
    print_consumer_metrics()