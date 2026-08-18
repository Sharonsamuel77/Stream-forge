import json
import os
import time
import os
from datetime import datetime

from confluent_kafka import (
    Consumer,
    KafkaException,
    KafkaError,
)

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


# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"

# IMPORTANT:
# All workers MUST use the same group ID.
GROUP_ID = "streamforge-state-workers"

DB_BASE_PATH = "data/rocksdb"

# Each worker can use a different Prometheus port.
METRICS_PORT = int(
    os.environ.get("STREAMFORGE_METRICS_PORT", "8000")
)


# ============================================================
# Prometheus
# ============================================================

start_metrics_server(METRICS_PORT)


# ============================================================
# Kafka Consumer
# ============================================================

consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,

    # Read old messages if there is no committed offset.
    "auto.offset.reset": "earliest",

    # We commit offsets manually after successful processing.
    "enable.auto.commit": False,
}
def update_worker_status():

    status = {
        "worker1": {
            "status": "active",
            "partition": 0,
            "last_seen": datetime.now().isoformat()
        },
        "worker2": {
            "status": "active",
            "partition": 1,
            "last_seen": datetime.now().isoformat()
        },
        "worker3": {
            "status": "active",
            "partition": 2,
            "last_seen": datetime.now().isoformat()
        }
    }

    with open("data/rocksdb/worker_status.json", "w") as f:
        json.dump(status, f, indent=4)

consumer = Consumer(consumer_config)


# ============================================================
# Partition State
# ============================================================

# Each Kafka partition gets its own RocksDB instance.
#
# Example:
#
# data/rocksdb/
#     partition_0/
#     partition_1/
#     partition_2/
#
# A worker only opens the databases for the partitions
# currently assigned to it.

partition_stores = {}
partition_processors = {}


# ============================================================
# Partition Assignment
# ============================================================

def on_assign(consumer, partitions):

    print()
    print("===================================")
    print("Partitions Assigned")
    print("===================================")

    for partition in partitions:

        partition_id = partition.partition

        print(
            f"Topic: {partition.topic} | "
            f"Partition: {partition_id} | "
            f"Offset: {partition.offset}"
        )

        # Open RocksDB for this partition.
        store = RocksDBStateStore(partition_id)

        processor = TemperatureProcessor(store)

        partition_stores[partition_id] = store
        partition_processors[partition_id] = processor

    consumer.assign(partitions)


# ============================================================
# Partition Revocation
# ============================================================

def on_revoke(consumer, partitions):

    print()
    print("===================================")
    print("Partitions Revoked")
    print("===================================")

    for partition in partitions:

        partition_id = partition.partition

        print(
            f"Topic: {partition.topic} | "
            f"Partition: {partition_id}"
        )

        # Close the state belonging to this partition.
        store = partition_stores.pop(
            partition_id,
            None
        )

        if store is not None:
            store.close()

        partition_processors.pop(
            partition_id,
            None
        )


# ============================================================
# Subscribe
# ============================================================

consumer.subscribe(
    [TOPIC],
    on_assign=on_assign,
    on_revoke=on_revoke,
)


# ============================================================
# Worker Startup
# ============================================================

print("===================================")
print("   StreamForge Telemetry Worker")
print("===================================")

print(f"Broker       : {KAFKA_BROKER}")
print(f"Topic        : {TOPIC}")
print(f"Consumer ID  : {GROUP_ID}")
print(f"State DB     : {DB_BASE_PATH}")
print("Processor     : 5-minute temperature average")
print("Monitoring    : Prometheus")
print(f"Metrics Port : {METRICS_PORT}")
print("Offsets       : Manual")

print()
print("Waiting for telemetry...")
print("Press Ctrl+C to stop")
print()


# ============================================================
# Main Worker Loop
# ============================================================

try:

    worker_up.set(1)

    def update_state_snapshot():

        snapshot = {}

        for key, value in state_store.db.items():

            avg_temp = 0

            if value["reading_count"] > 0:
                avg_temp = round(
                    value["temperature_sum"]
                    / value["reading_count"],
                    2
            )

            snapshot[str(key).replace("truck:", "")] = {
                "avg_temperature": avg_temp,
                "readings": value["reading_count"]
            }

        with open(
            "data/state_snapshot.json",
            "w"
        ) as f:
            json.dump(snapshot, f, indent=4)

    while True:

        # ----------------------------------------------------
        # Poll Kafka
        # ----------------------------------------------------
        update_worker_status()

        message = consumer.poll(1.0)

        # ----------------------------------------------------
        # No message
        # ----------------------------------------------------

        if message is None:
            continue

        # ----------------------------------------------------
        # Kafka error
        # ----------------------------------------------------

        if message.error():

            if message.error().code() == KafkaError._PARTITION_EOF:
                continue

            print(
                f"Kafka error: {message.error()}"
            )

            processing_errors_total.inc()
            continue


        # ----------------------------------------------------
        # Message received
        # ----------------------------------------------------

        messages_consumed_total.inc()

        start_time = time.perf_counter()

        partition_id = message.partition()


        try:

            # ------------------------------------------------
            # Get processor for this partition
            # ------------------------------------------------

            processor = partition_processors.get(
                partition_id
            )

            if processor is None:

                print(
                    f"No processor available for "
                    f"partition {partition_id}"
                )

                processing_errors_total.inc()

                continue


            # ------------------------------------------------
            # Decode message
            # ------------------------------------------------

            data = json.loads(
                message.value().decode("utf-8")
            )


            # ------------------------------------------------
            # Extract data
            # ------------------------------------------------

            truck_id = data["truck_id"]

            temperature = float(
                data["temperature"]
            )

            timestamp = data["timestamp"]


            # ------------------------------------------------
            # Validate temperature
            # ------------------------------------------------

            if temperature < -50 or temperature > 100:

                print(
                    f"Invalid temperature ignored: "
                    f"Truck {truck_id} -> "
                    f"{temperature}°C"
                )

                processing_errors_total.inc()

                # Invalid data should not remain
                # permanently in the Kafka group.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                continue

            # ------------------------------------------------
            # Active truck metric
            # ------------------------------------------------

            active_trucks.set(
                len(
                    {
                        key
                        for key in partition_processors.keys()
                    }
                )
            )


            # ------------------------------------------------
            # Process event
            # ------------------------------------------------

            result = processor.process(
                truck_id,
                temperature,
                timestamp,
            )
            update_state_snapshot()

            # ------------------------------------------------
            # Processing output
            # ------------------------------------------------

            print(
                f"Processed | "
                f"truck={truck_id} | "
                f"temperature={temperature:.2f}°C | "
                f"partition={partition_id} | "
                f"offset={message.offset()}"
            )


            # ------------------------------------------------
            # 5-minute window completed
            # ------------------------------------------------

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


            # ------------------------------------------------
            # Processing successful
            # ------------------------------------------------

            messages_processed_total.inc()


            # ------------------------------------------------
            # Processing latency
            # ------------------------------------------------

            latency = (
                time.perf_counter()
                - start_time
            )

            processing_latency_seconds.observe(
                latency
            )


            # ------------------------------------------------
            # Manual Kafka offset commit
            # ------------------------------------------------

            consumer.commit(
                message=message,
                asynchronous=False
            )


        # ----------------------------------------------------
        # Invalid message
        # ----------------------------------------------------

        except (
            json.JSONDecodeError,
            KeyError,
            ValueError,
        ) as error:

            processing_errors_total.inc()
            print(
                f"Invalid message ignored: {error}"
            )


        # ----------------------------------------------------
        # Unexpected error
        # ----------------------------------------------------

        except Exception as error:

            processing_errors_total.inc()
            print(
                f"Processing error: {error}"
            )


# ============================================================
# Shutdown
# ============================================================

except KeyboardInterrupt:

    print()
    print("Stopping StreamForge worker...")


except KafkaException as error:

    print(
        f"Kafka exception: {error}"
    )


finally:

    worker_up.set(0)

    # Close all partition state stores.
    for store in partition_stores.values():

        try:
            store.close()

        except Exception:
            pass

    partition_stores.clear()
    partition_processors.clear()

    consumer.close()

    print("Worker stopped.")