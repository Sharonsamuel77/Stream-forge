from confluent_kafka import Consumer
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from monitoring.metrics_server import (
    start_metrics_server,
    messages_consumed_total,
    messages_processed_total,
    processing_errors_total,
    active_trucks,
    processing_latency_seconds,
    worker_up,
)

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

# Start Prometheus metrics server
start_metrics_server()

print("StreamForge Worker started...")
print(f"Listening to topic: {TOPIC}")

try:
    worker_up.set(1)

    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Kafka error: {message.error()}")
            processing_errors_total.inc()
            continue

        # Count every successfully received Kafka message
        messages_consumed_total.inc()

        try:
            # Measure message processing latency
            with processing_latency_seconds.time():

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
                    processing_errors_total.inc()
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

                # Update active truck count
                active_trucks.set(len(truck_readings))

                # Count successfully processed message
                messages_processed_total.inc()

        except (json.JSONDecodeError, KeyError, ValueError) as error:
            processing_errors_total.inc()
            print(f"Invalid message ignored: {error}")

        except Exception as error:
            processing_errors_total.inc()
            print(f"Processing error: {error}")

except KeyboardInterrupt:
    print("\nWorker stopped.")

finally:
    worker_up.set(0)
    active_trucks.set(0)
    consumer.close()
    print("Kafka consumer closed.")