from confluent_kafka import Consumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": "streamforge-workers",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([TOPIC])

# Store recent readings for each truck
truck_readings = defaultdict(deque)

print("StreamForge Worker started...")
print(f"Listening to topic: {TOPIC}")

try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Kafka error: {message.error()}")
            continue

        try:
            data = json.loads(message.value().decode("utf-8"))

            truck_id = data["truck_id"]
            temperature = float(data["temperature"])
            timestamp = datetime.fromisoformat(data["timestamp"])

            # Basic filtering
            if temperature < -50 or temperature > 100:
                print(
                    f"Invalid temperature ignored: "
                    f"Truck {truck_id} → {temperature}°C"
                )
                continue

            # Add the new reading
            truck_readings[truck_id].append(
                (timestamp, temperature)
            )

            # Keep only the last 5 minutes
            cutoff_time = timestamp - timedelta(minutes=5)

            while (
                truck_readings[truck_id]
                and truck_readings[truck_id][0][0] < cutoff_time
            ):
                truck_readings[truck_id].popleft()

            # Calculate average
            readings = truck_readings[truck_id]

            if readings:
                average = sum(
                    temp for _, temp in readings
                ) / len(readings)

                print(
                    f"Truck: {truck_id} | "
                    f"5-min Average: {average:.2f}°C | "
                    f"Readings: {len(readings)}"
                )

        except (json.JSONDecodeError, KeyError, ValueError) as error:
            print(f"Invalid message ignored: {error}")

except KeyboardInterrupt:
    print("\nWorker stopped.")

finally:
    consumer.close()