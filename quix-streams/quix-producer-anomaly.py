import sys
import os
import time
from quixstreams import Application

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from datasource.TemperatureReadings import TemperatureEventGenerator

# Function for the Quix producer, which generates temperature events and sends them to the topic
def quix_producer():
    _app = Application(broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"))
    topic = _app.topic(name="quix-temperature-readings")
    event_generator = TemperatureEventGenerator()

    with _app.get_producer() as producer:
        while event := event_generator.generate_event():
            event = topic.serialize(**event)
            print(f"Quix Producer: Producing event for MID {event.key}, {event.value}")
            producer.produce(key=event.key, value=event.value, topic=topic.name)
            time.sleep(0.2) # Simulate real-time event production with a delay

    print("Quix Producer: Stopping producing.")

if __name__ == "__main__":
    quix_producer()