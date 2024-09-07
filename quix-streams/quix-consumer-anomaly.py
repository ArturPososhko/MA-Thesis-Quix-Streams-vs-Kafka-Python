import os
from quixstreams import Application

# Initialize the Quix application
app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="quix-temperature-alerter",
    auto_offset_reset="earliest",
)

# Input and output topics
temperature_readings_topic = app.topic(name="quix-temperature-readings")
alerts_topic = app.topic(name="quix-alerts")

# Function to check if an alert should be sent
def should_alert(window_value: int, key, timestamp, headers):
    if window_value >= 90:
        print(f"Quix Consumer: Alerting for MID {key}: Average Temperature {window_value}")
        return True

# Process the incoming temperature data, apply windowing, and filter for alerts
sdf = app.dataframe(topic=temperature_readings_topic)
sdf = sdf.apply(lambda data: data["Temperature_C"])
sdf = sdf.hopping_window(duration_ms=5000, step_ms=1000).mean().current()
sdf = sdf.apply(lambda result: round(result["value"], 2)).filter(
    should_alert, metadata=True
)
sdf = sdf.to_topic(alerts_topic)

if __name__ == "__main__":
    app.run(sdf)