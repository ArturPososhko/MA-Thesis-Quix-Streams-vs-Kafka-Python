import os
import time
import psutil
import numpy as np
from matplotlib import pyplot as plt
from quixstreams import Application
from threading import Thread

# Configurable durations for the consumer
first_run_duration = 10  # First run of the consumer in seconds
downtime_duration = 20    # Downtime duration in seconds
second_run_duration = 10  # Second run of the consumer in seconds

# Global variables for consumer metrics and timers
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
run_first_message_time = None
is_downtime = False

def print_system_capacity():
    cpu_count = psutil.cpu_count(logical=True)
    total_memory = psutil.virtual_memory().total / (1024 * 1024)
    print(f"CPU Count: {cpu_count}")
    print(f"Total Memory: {total_memory:.2f} MB")

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

        for i in range(cpu_per_core_count):
            cpu_per_core_usage[i].append(per_core_cpu[i])

        time.sleep(1)

# Quix Streams Consumer
def quix_consumer(run_duration, run_name):
    global messages_consumed, latency_stats, message_window_start, throughput_data, throughput_time, run_first_message_time, first_message_time, is_downtime
    _app = Application(broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
                       consumer_group="quix-fault-tolerance-consumer",
                       auto_offset_reset="latest")
    
    topic = _app.topic(name="quix-5000-events-per-sec")

    def on_message_consume(message):
        global messages_consumed, latency_stats, message_window_start, throughput_data, throughput_time, first_message_time, run_first_message_time

        # Skip processing if in downtime
        if is_downtime:
            return

        # Track the first message of the entire session for resource monitoring
        if first_message_time is None:
            first_message_time = time.time()
            print("First global message received. Resource tracking started.")

        # Track the first message of the current run
        if run_first_message_time is None:
            run_first_message_time = time.time()
            print(f"First message received in {run_name}. Timer started for this run.")
            message_window_start = run_first_message_time

        # Calculate latency
        production_timestamp = message["Timestamp"]
        consumption_timestamp = time.time_ns()
        latency = (consumption_timestamp - production_timestamp) / 1_000_000  # in ms
        latency_stats.append(latency)

        messages_consumed += 1

        # Log consumption stats every second
        if time.time() - message_window_start >= 1:
            throughput_data.append(messages_consumed)
            throughput_time.append(time.time() - first_message_time)
            print(f"Consumed {messages_consumed} messages in the last second")
            messages_consumed = 0
            message_window_start = time.time()

        # Stop the consumer after the specified run duration
        if time.time() - run_first_message_time >= run_duration:
            print(f"{run_name} has completed {run_duration} seconds. Stopping the consumer.")
            _app.stop()

    # Subscribe the consumer to the topic and set the callback
    sdf = _app.dataframe(topic=topic)
    sdf = sdf.apply(on_message_consume)

    # Run the consumer
    _app.run(sdf)

# Function to print consumer metrics and generate graphs
def print_consumer_metrics():
    avg_cpu = np.mean(cpu_usage)
    avg_memory = np.mean(memory_usage)
    avg_latency = np.mean(latency_stats)
    avg_throughput = np.mean(throughput_data)

    print(f"CPU Usage (%) - Avg: {avg_cpu:.2f}")
    print(f"Memory Usage (%) - Avg: {avg_memory:.2f}")
    print(f"Latency (ms) - Avg: {avg_latency:.2f}")
    print(f"Throughput (messages/sec) - Avg: {avg_throughput:.2f}")

    if len(cpu_per_core_usage) > 0:
        per_core_avg = [np.mean(core) for core in cpu_per_core_usage]
        print("Average CPU usage per core:")
        for i, avg in enumerate(per_core_avg):
            print(f"Core {i}: {avg:.2f}%")

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
    plt.savefig("quix_consumer_fault_tolerance.png")
    plt.show()

if __name__ == "__main__":
    print_system_capacity()

    # Start resource monitoring in a separate thread
    resource_monitor_thread = Thread(target=monitor_resources)
    resource_monitor_thread.daemon = True
    resource_monitor_thread.start()

    # Start the first consumer run
    print(f"Starting the first consumer run for {first_run_duration} seconds.")
    is_downtime = False
    run_first_message_time = None
    quix_consumer(first_run_duration, "First run")

    # Pause for the downtime duration
    print(f"Pausing consumer for {downtime_duration} seconds.")
    is_downtime = True
    for i in range(downtime_duration): # Zero throughput for each second during downtime
        throughput_data.append(0)
        throughput_time.append(time.time() - first_message_time)
        time.sleep(1)

    # Start the second consumer run
    print(f"Starting the second consumer run for {second_run_duration} seconds.")
    is_downtime = False
    run_first_message_time = None
    quix_consumer(second_run_duration, "Second run")

    # Print and plot the consumer metrics
    print_consumer_metrics()