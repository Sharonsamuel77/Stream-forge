from confluent_kafka import Consumer
import json

from state.rocksdb_store import RocksDBStateStore
from state.temperature_processor import TemperatureProcessor


# -----------------------------
# Kafka Configuration
# -----------------------------

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"
GROUP_ID = "streamforge-state-workers"


# -----------------------------
# Kafka Consumer
# -----------------------------

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])


# -----------------------------
# Persistent State
# -----------------------------

store = RocksDBStateStore("data/rocksdb")
processor = TemperatureProcessor(store)


# -----------------------------
# Worker Startup
# -----------------------------

print("StreamForge Worker started...")
print(f"Listening to topic: {TOPIC}")
print("State store: RocksDB")
print("Processor: 5-minute temperature average")
print("Kafka offset commits: MANUAL")


# -----------------------------
# Main Processing Loop
# -----------------------------

try:
    while True:

        message = consumer.poll(1.0)

        # No message available
        if message is None:
            continue

        # Kafka error
        if message.error():
            print(f"Kafka error: {message.error()}")
            continue

        try:
            # -----------------------------
            # Decode Kafka message
            # -----------------------------

            data = json.loads(
                message.value().decode("utf-8")
            )

            truck_id = data["truck_id"]
            temperature = float(data["temperature"])
            timestamp = data["timestamp"]


            # -----------------------------
            # Validate temperature
            # -----------------------------

            if temperature < -50 or temperature > 100:

                print(
                    f"Invalid temperature ignored: "
                    f"Truck {truck_id} -> {temperature}°C"
                )

                # Since this message is intentionally ignored,
                # commit its offset so it isn't repeatedly processed.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                continue


            # -----------------------------
            # Process using RocksDB state
            # -----------------------------

            result = processor.process(
                truck_id,
                temperature,
                timestamp
            )


            # -----------------------------
            # Commit Kafka offset
            # -----------------------------
            #
            # IMPORTANT:
            # State is written to RocksDB first.
            # Only after successful processing do we
            # commit the Kafka offset.
            #

            consumer.commit(
                message=message,
                asynchronous=False
            )


            # -----------------------------
            # Print completed 5-minute window
            # -----------------------------

            if result is not None:

                print(
                    f"5-MIN AVERAGE | "
                    f"Truck: {result['truck_id']} | "
                    f"Window: "
                    f"{result['window_start']} -> "
                    f"{result['window_end']} | "
                    f"Average: "
                    f"{result['average_temperature']:.2f}°C"
                )


        except (
            json.JSONDecodeError,
            KeyError,
            ValueError
        ) as error:

            print(
                f"Invalid message ignored: {error}"
            )

            # Do not commit malformed messages here.
            # This prevents silently losing bad data.


except KeyboardInterrupt:

    print("\nWorker stopped.")


finally:

    consumer.close()
    store.close()