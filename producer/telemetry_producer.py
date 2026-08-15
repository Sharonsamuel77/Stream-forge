from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime
import argparse


# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})


# -----------------------------
# Simulation configuration
# -----------------------------
parser = argparse.ArgumentParser()

parser.add_argument(
    "--trucks",
    type=int,
    default=10,
    help="Number of trucks to simulate"
)

parser.add_argument(
    "--interval",
    type=float,
    default=1.0,
    help="Seconds between batches"
)

args = parser.parse_args()

NUM_TRUCKS = args.trucks
INTERVAL = args.interval

def generate_truck_data(truck_id):
    """Generate one temperature reading for a truck."""

    temperature = round(random.uniform(25.0, 40.0), 2)

    return {
        "truck_id": truck_id,
        "temperature": temperature,
        "timestamp": datetime.now().isoformat()
    }


def delivery_report(err, message):
    """Called by Kafka when a message is delivered or fails."""

    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered: truck={message.key().decode()} "
            f"partition={message.partition()} "
            f"offset={message.offset()}"
        )


print("===================================")
print("      StreamForge Producer")
print("===================================")
print(f"Simulating {NUM_TRUCKS} trucks")
print(f"Sending one reading every {INTERVAL} second(s)")
print("Press Ctrl+C to stop")
print()


try:
    while True:

        for truck_id in range(1, NUM_TRUCKS + 1):

            event = generate_truck_data(truck_id)

            producer.produce(
                TOPIC,
                key=str(truck_id),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report
            )

        # Give Kafka time to deliver queued messages
        producer.poll(0)

        print(
            f"Generated telemetry for {NUM_TRUCKS} trucks "
            f"at {datetime.now().strftime('%H:%M:%S')}"
        )

        time.sleep(INTERVAL)


except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    # Wait for all outstanding messages to be delivered
    producer.flush()
    print("Producer stopped.")