import json
import time

from confluent_kafka import Consumer, KafkaException, KafkaError

from monitoring.metrics_server import (
    start_metrics_server,
    messages_consumed_total,
    messages_processed_total,
    processing_errors_total,
    active_trucks,
    processing_latency_seconds,
    worker_up,
)

from state.rocksdb_store import RocksDBStateStore
from state.temperature_processor import TemperatureProcessor


# ============================================
# Configuration
# ============================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"
GROUP_ID = "streamforge-workers"

DB_PATH = "data/rocksdb"


# ============================================
# Start Prometheus Metrics Server
# ============================================

start_metrics_server()


# ============================================
# Kafka Consumer Configuration
# ============================================

consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(consumer_config)


# ============================================
# RocksDB State Store
# ============================================

state_store = RocksDBStateStore(DB_PATH)

processor = TemperatureProcessor(state_store)


# ============================================
# Active Truck Tracking
# ============================================

active_truck_ids = set()


# ============================================
# Kafka Partition Assignment
# ============================================

def on_assign(consumer, partitions):

    print()
    print("===================================")
    print("Partitions Assigned")
    print("===================================")

    for partition in partitions:

        print(
            f"Topic: {partition.topic} | "
            f"Partition: {partition.partition} | "
            f"Offset: {partition.offset}"
        )

    consumer.assign(partitions)


# ============================================
# Kafka Partition Revocation
# ============================================

def on_revoke(consumer, partitions):

    print()
    print("===================================")
    print("Partitions Revoked")
    print("===================================")

    for partition in partitions:

        print(
            f"Topic: {partition.topic} | "
            f"Partition: {partition.partition}"
        )


# ============================================
# Subscribe to Kafka Topic
# ============================================

consumer.subscribe(
    [TOPIC],
    on_assign=on_assign,
    on_revoke=on_revoke,
)


# ============================================
# Worker Startup
# ============================================

print("===================================")
print("   StreamForge Telemetry Worker")
print("===================================")

print(f"Broker      : {KAFKA_BROKER}")
print(f"Topic       : {TOPIC}")
print(f"Consumer ID : {GROUP_ID}")
print(f"State DB    : {DB_PATH}")

print("Waiting for telemetry...")
print("Press Ctrl+C to stop")
print()


# ============================================
# Main Worker Loop
# ============================================

try:

    worker_up.set(1)

    while True:

        # ------------------------------------
        # Poll Kafka
        # ------------------------------------

        message = consumer.poll(1.0)

        if message is None:
            continue


        # ------------------------------------
        # Kafka Error Handling
        # ------------------------------------

        if message.error():

            if message.error().code() == KafkaError._PARTITION_EOF:

                continue

            print(
                f"Kafka error: {message.error()}"
            )

            processing_errors_total.inc()

            continue


        # ------------------------------------
        # Message Received
        # ------------------------------------

        messages_consumed_total.inc()

        start_time = time.perf_counter()


        try:

            # =================================
            # Decode Kafka Message
            # =================================

            data = json.loads(
                message.value().decode("utf-8")
            )


            # =================================
            # Extract Event Data
            # =================================

            truck_id = data["truck_id"]

            temperature = float(
                data["temperature"]
            )

            timestamp = data["timestamp"]


            # =================================
            # Validate Temperature
            # =================================

            if temperature < -50 or temperature > 100:

                print(
                    f"Invalid temperature ignored: "
                    f"Truck {truck_id} -> "
                    f"{temperature}°C"
                )

                processing_errors_total.inc()

                # Commit invalid message so it
                # isn't processed repeatedly.
                consumer.commit(
                    message=message
                )

                continue


            # =================================
            # Track Active Trucks
            # =================================

            active_truck_ids.add(
                truck_id
            )

            active_trucks.set(
                len(active_truck_ids)
            )


            # =================================
            # Process Temperature
            # =================================

            result = processor.process(
                truck_id,
                temperature,
                timestamp,
            )


            # =================================
            # Print Processing Information
            # =================================

            print(
                f"Processed | "
                f"truck={truck_id} | "
                f"temperature={temperature:.2f}°C | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )


            # =================================
            # Check Completed 5-Minute Window
            # =================================

            if result is not None:

                print(
                    f"5-min Average | "
                    f"truck={result['truck_id']} | "
                    f"window="
                    f"{result['window_start']} -> "
                    f"{result['window_end']} | "
                    f"average="
                    f"{result['average_temperature']:.2f}°C"
                )


            # =================================
            # Successfully Processed
            # =================================

            messages_processed_total.inc()


            # =================================
            # Processing Latency
            # =================================

            latency = (
                time.perf_counter()
                - start_time
            )

            processing_latency_seconds.observe(
                latency
            )


            # =================================
            # Manual Kafka Commit
            # =================================

            consumer.commit(
                message=message
            )


        # =====================================
        # Invalid Message
        # =====================================

        except (
            json.JSONDecodeError,
            KeyError,
            ValueError,
        ) as error:

            processing_errors_total.inc()

            print(
                f"Invalid message ignored: "
                f"{error}"
            )


        # =====================================
        # Unexpected Processing Error
        # =====================================

        except Exception as error:

            processing_errors_total.inc()

            print(
                f"Processing error: "
                f"{error}"
            )


# ============================================
# Graceful Shutdown
# ============================================

except KeyboardInterrupt:

    print(
        "\nStopping StreamForge worker..."
    )


except KafkaException as error:

    print(
        f"\nKafka exception: {error}"
    )


finally:

    worker_up.set(0)

    state_store.close()

    consumer.close()

    print(
        "Worker stopped."
    )