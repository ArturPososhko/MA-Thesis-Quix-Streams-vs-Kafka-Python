import random
import time

class TemperatureEventGenerator:
    probabilities_normal = {
        40: [0, 0, 100],
        50: [20, 30, 50],
        60: [30, 40, 30],
        70: [40, 50, 10],
        80: [80, 10, 10],
        90: [100, 0, 0],
        100: [100, 0, 0],  # Add this line to handle the 100°C case
    }

    probabilities_issue = {
        40: [0, 0, 100],
        50: [0, 10, 90],
        60: [5, 15, 80],
        70: [5, 20, 75],
        80: [5, 20, 75],
        90: [10, 20, 70],
        100: [10, 20, 70],  # Add this line to handle the 100°C case
    }

    def __init__(self, max_events_per_second=5):
        self.event_count = 0
        self.machine_temps = {0: 66, 1: 58, 2: 62}
        self.machine_types = {
            0: self.probabilities_normal,
            1: self.probabilities_normal,
            2: self.probabilities_issue,
        }
        self.max_events_per_second = max_events_per_second  # Limit the number of events per second
        self.message_window_start = time.time()  # Track the start of the current time window
        self.messages_sent = 0  # Track how many events are generated in the current time window

    def update_machine_temp(self, machine_id):
        # Cap the temperature at 100°C
        if self.machine_temps[machine_id] < 100:
            self.machine_temps[machine_id] += random.choices(
                [-1, 0, 1],
                self.machine_types[machine_id][(self.machine_temps[machine_id] // 10) * 10],
            )[0]
        else:
            self.machine_temps[machine_id] = 100  # Ensure the temperature stays at 100°C

    def generate_event(self):
        machine_id = self.event_count % 3
        self.update_machine_temp(machine_id)
        event_out = {
            "key": str(machine_id),
            "value": {
                "Temperature_C": self.machine_temps[machine_id],
                "Timestamp": time.time_ns(),
            },
        }
        self.event_count += 1
        return event_out

    def generate_limited_event(self):
        # Throttle event generation based on max_events_per_second
        if self.messages_sent >= self.max_events_per_second:
            elapsed_time = time.time() - self.message_window_start
            if elapsed_time < 1:
                time.sleep(1 - elapsed_time)  # Sleep to limit to max events per second
            self.messages_sent = 0  # Reset for the next time window
            self.message_window_start = time.time()  # Restart the window

        event = self.generate_event()
        self.messages_sent += 1
        return event


if __name__ == "__main__":
    event_generator = TemperatureEventGenerator(max_events_per_second=5)  # Limit to 5 events per second
    while True:  # Infinite loop
        event = event_generator.generate_limited_event()
        print(event)
