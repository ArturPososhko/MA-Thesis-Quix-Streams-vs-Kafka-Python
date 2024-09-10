import sys
import os
import time
import psutil
import numpy as np
import logging
from matplotlib import pyplot as plt
from quixstreams import Application
from threading import Thread

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from datasource.TemperatureReadings import TemperatureEventGenerator

# Configurable duration for the producer
uptime_duration = 90  # Uptime of the producer in seconds

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
log_file = "quix_producer_5000_events_per_sec_logs.txt"
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

# Quix Streams Producer
def quix_producer():
    global messages_sent, message_window_start, first_message_time, throughput_data, throughput_time
    
    _app = Application(broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"))
    topic = _app.topic(name="quix-5000-events-per-sec")
    temperature_event_generator = TemperatureEventGenerator(max_events_per_second=5000)
    producer = _app.get_producer()
    
    # Set the start time for the first message and window tracking
    start_time = time.time()
    first_message_time = start_time
    message_window_start = time.time()

    # Produce messages for the specified run duration
    while time.time() - start_time < uptime_duration: 
        event = temperature_event_generator.generate_limited_event()
        event = topic.serialize(**event)
        producer.produce(key=event.key, value=event.value, topic=topic.name)
        messages_sent += 1

        # Log producer stats every second
        if time.time() - message_window_start >= 1:
            throughput_data.append(messages_sent)
            throughput_time.append(time.time() - first_message_time)
            logging.info(f"Produced {messages_sent} messages in the last second")
            messages_sent = 0
            message_window_start = time.time()

    logging.info("Quix producer stopped.")

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
    plt.savefig("quix_producer_performance_metrics.png")
    plt.show()

if __name__ == "__main__":
    print_system_capacity()

    # Start resource monitoring in a separate thread
    resource_monitor_thread = Thread(target=monitor_resources)
    resource_monitor_thread.daemon = True  # Runs in the background
    resource_monitor_thread.start()

    # Start the producer run
    quix_producer()

    # Print and plot the producer metrics
    print_producer_metrics()