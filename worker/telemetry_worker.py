import json
import os
import time
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

# ALL workers MUST use the same group.
GROUP_ID = "streamforge-state-workers"

DB_BASE_PATH = "data/rocksdb"

# Each worker gets its own identity.
WORKER_ID = os.environ.get(
    "STREAMFORGE_WORKER_ID",
    f"worker-{os.getpid()}"
)

# Each worker should use a different metrics port.
METRICS_PORT = int(
    os.environ.get(
        "STREAMFORGE_METRICS_PORT",
        "8000"
    )
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

    # Start from earliest offset when no committed offset exists.
    "auto.offset.reset": "earliest",

    # Manual commit after successful processing.
    "enable.auto.commit": False,

    # Identify the worker clearly in Kafka.
    "client.id": WORKER_ID,

    # Give processing enough time before Kafka considers
    # this worker dead.
    "session.timeout.ms": 10000,
    "heartbeat.interval.ms": 3000,
}


# ============================================================
# Partition State
# ============================================================

# Only partitions currently owned by THIS worker exist here.
partition_stores = {}
partition_processors = {}


# ============================================================
# Worker Status
# ============================================================

def update_worker_status():
    """
    Report the REAL Kafka partition assignment for this worker.

    Kafka dynamically assigns partitions to consumers in the same
    consumer group. Do not hard-code worker1 -> partition 0, etc.
    """

    worker_id = os.environ.get(
        "STREAMFORGE_WORKER_ID",
        f"worker-{os.getpid()}"
    )

    assigned_partitions = sorted(
        partition.partition
        for partition in consumer.assignment()
    )

    status = {
        worker_id: {
            "status": "active",
            "pid": os.getpid(),
            "partitions": assigned_partitions,
            "partition_count": len(assigned_partitions),
            "last_seen": datetime.now().isoformat()
        }
    }

    os.makedirs(
        "data/rocksdb",
        exist_ok=True
    )

    status_file = "data/rocksdb/worker_status.json"

    # --------------------------------------------------------
    # Preserve the status of other workers.
    # --------------------------------------------------------

    existing_status = {}

    try:
        if os.path.exists(status_file):

            with open(
                status_file,
                "r"
            ) as f:

                existing_status = json.load(f)

    except (
        json.JSONDecodeError,
        OSError
    ):
        existing_status = {}

    # --------------------------------------------------------
    # Update THIS worker's status.
    # --------------------------------------------------------

    existing_status[worker_id] = status[worker_id]

    # --------------------------------------------------------
    # Remove stale worker entries.
    #
    # A worker is considered stale if it has not updated
    # its status for more than 15 seconds.
    # --------------------------------------------------------

    now = datetime.now()

    stale_workers = []

    for existing_worker_id, worker_data in existing_status.items():

        if existing_worker_id == worker_id:
            continue

        try:

            last_seen = datetime.fromisoformat(
                worker_data["last_seen"]
            )

            age = (
                now - last_seen
            ).total_seconds()

            if age > 15:
                stale_workers.append(
                    existing_worker_id
                )

        except (
            KeyError,
            ValueError,
            TypeError
        ):
            stale_workers.append(
                existing_worker_id
            )

    for stale_worker in stale_workers:
        existing_status.pop(
            stale_worker,
            None
        )

    # --------------------------------------------------------
    # Write updated worker status.
    # --------------------------------------------------------

    try:

        with open(
            status_file,
            "w"
        ) as f:

            json.dump(
                existing_status,
                f,
                indent=4
            )

    except OSError as error:

        print(
            f"Failed to update worker status: {error}"
        )


# ============================================================
# Kafka Consumer
# ============================================================

consumer = Consumer(consumer_config)


# ============================================================
# Partition Assignment
# ============================================================

def on_assign(consumer, partitions):

    print()
    print("===================================")
    print(f"{WORKER_ID} - Partitions Assigned")
    print("===================================")

    # Safety:
    # If Kafka assigns a partition again, don't leave an old
    # RocksDB handle open.
    for partition_id, store in list(
        partition_stores.items()
    ):

        if partition_id not in {
            p.partition for p in partitions
        }:

            try:
                store.close()
            except Exception:
                pass

            partition_stores.pop(
                partition_id,
                None
            )

            partition_processors.pop(
                partition_id,
                None
            )

    for partition in partitions:

        partition_id = partition.partition

        print(
            f"Worker      : {WORKER_ID}"
        )

        print(
            f"Topic       : {partition.topic}"
        )

        print(
            f"Partition   : {partition_id}"
        )

        print(
            f"Kafka Offset: {partition.offset}"
        )

        try:

            # ------------------------------------------------
            # Open partition-specific RocksDB
            # ------------------------------------------------

            store = RocksDBStateStore(
                partition_id
            )

            # ------------------------------------------------
            # Create processor using recovered state
            # ------------------------------------------------

            processor = TemperatureProcessor(
                store
            )

            partition_stores[
                partition_id
            ] = store

            partition_processors[
                partition_id
            ] = processor

            print(
                f"{WORKER_ID} owns partition "
                f"{partition_id}"
            )

        except Exception as error:

            print(
                f"Failed to initialize partition "
                f"{partition_id}: {error}"
            )

    # Kafka now officially assigns these partitions.
    consumer.assign(partitions)

    update_worker_status()

    print(
        f"Current assignment: "
        f"{sorted(partition_processors.keys())}"
    )

    print()


# ============================================================
# Partition Revocation
# ============================================================

def on_revoke(consumer, partitions):

    print()
    print("===================================")
    print(f"{WORKER_ID} - Partitions Revoked")
    print("===================================")

    for partition in partitions:

        partition_id = partition.partition

        print(
            f"Revoking partition {partition_id} "
            f"from {WORKER_ID}"
        )

        store = partition_stores.pop(
            partition_id,
            None
        )

        if store is not None:

            try:
                store.close()

            except Exception as error:

                print(
                    f"Error closing RocksDB "
                    f"partition {partition_id}: "
                    f"{error}"
                )

        partition_processors.pop(
            partition_id,
            None
        )

    update_worker_status()

    print(
        f"{WORKER_ID} remaining partitions: "
        f"{sorted(partition_processors.keys())}"
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

print(f"Worker ID    : {WORKER_ID}")
print(f"PID          : {os.getpid()}")
print(f"Broker       : {KAFKA_BROKER}")
print(f"Topic        : {TOPIC}")
print(f"Consumer ID  : {GROUP_ID}")
print(f"State DB     : {DB_BASE_PATH}")
print("Processor     : 5-minute rolling average")
print("Monitoring    : Prometheus")
print(f"Metrics Port : {METRICS_PORT}")
print("Offsets       : Manual")
print("Assignment    : Kafka dynamic")
print()

print("Waiting for telemetry...")
print("Press Ctrl+C to stop")
print()


# ============================================================
# State Snapshot
# ============================================================

def update_state_snapshot():

    snapshot = {}

    for partition_id, store in partition_stores.items():

        try:

            for key, value in store.db.items():

                if value["reading_count"] > 0:

                    avg_temp = round(
                        value["temperature_sum"]
                        / value["reading_count"],
                        2
                    )

                else:

                    avg_temp = 0

                truck_id = str(key).replace(
                    "truck:",
                    ""
                )

                snapshot[truck_id] = {
                    "avg_temperature": avg_temp,
                    "readings": value["reading_count"],
                    "partition": partition_id,
                    "worker_id": WORKER_ID,
                }

        except Exception as error:

            print(
                f"Snapshot error for partition "
                f"{partition_id}: {error}"
            )

    os.makedirs(
        "data",
        exist_ok=True
    )

    # Worker-specific snapshot prevents workers from
    # overwriting each other.
    snapshot_file = (
        f"data/state_snapshot_{WORKER_ID}.json"
    )

    with open(
        snapshot_file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            snapshot,
            f,
            indent=4
        )


# ============================================================
# Main Worker Loop
# ============================================================

try:

    worker_up.set(1)

    while True:

        # ----------------------------------------------------
        # Update worker status
        # ----------------------------------------------------

        update_worker_status()

        # ----------------------------------------------------
        # Poll Kafka
        # ----------------------------------------------------

        message = consumer.poll(1.0)

        if message is None:
            continue

        # ----------------------------------------------------
        # Kafka error
        # ----------------------------------------------------

        if message.error():

            if (
                message.error().code()
                == KafkaError._PARTITION_EOF
            ):
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
            # Verify this worker owns the partition
            # ------------------------------------------------

            processor = partition_processors.get(
                partition_id
            )

            if processor is None:

                print(
                    f"ERROR: {WORKER_ID} received "
                    f"partition {partition_id}, "
                    f"but it is not assigned."
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

            if (
                temperature < -50
                or temperature > 100
            ):

                print(
                    f"Invalid temperature ignored: "
                    f"Truck {truck_id} -> "
                    f"{temperature} C"
                )

                processing_errors_total.inc()

                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                continue

            # ------------------------------------------------
            # Active truck count
            # ------------------------------------------------

            truck_count = 0

            try:

                for store in partition_stores.values():

                    truck_count += store.count()

            except Exception:
                truck_count = 0

            active_trucks.set(
                truck_count
            )

            # ------------------------------------------------
            # Process event
            # ------------------------------------------------

            result = processor.process(
                truck_id,
                temperature,
                timestamp,
            )

            # ------------------------------------------------
            # Update state snapshot
            # ------------------------------------------------

            update_state_snapshot()

            # ------------------------------------------------
            # Processing output
            # ------------------------------------------------

            print(
                f"Processed | "
                f"worker={WORKER_ID} | "
                f"truck={truck_id} | "
                f"temperature={temperature:.2f} C | "
                f"partition={partition_id} | "
                f"offset={message.offset()}"
            )

            # ------------------------------------------------
            # Rolling window result
            # ------------------------------------------------

            if result is not None:

                print(
                    f"5-MIN ROLLING AVERAGE | "
                    f"Worker: {WORKER_ID} | "
                    f"Truck: {result['truck_id']} | "
                    f"Window: "
                    f"{result['window_start']} -> "
                    f"{result['window_end']} | "
                    f"Average: "
                    f"{result['average_temperature']:.2f} C | "
                    f"Readings: "
                    f"{result['reading_count']}"
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
            # Commit ONLY after successful state update
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
        # Unexpected processing error
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
    print(
        f"Stopping StreamForge worker "
        f"{WORKER_ID}..."
    )


except KafkaException as error:

    print(
        f"Kafka exception: {error}"
    )


finally:

    worker_up.set(0)

    # --------------------------------------------------------
    # Mark worker inactive
    # --------------------------------------------------------

    try:

        status_file = (
            f"{DB_BASE_PATH}/worker_status_{WORKER_ID}.json"
        )

        status = {
            "worker_id": WORKER_ID,
            "status": "stopped",
            "pid": os.getpid(),
            "partitions": [],
            "partition_count": 0,
            "last_seen": datetime.now().isoformat(),
            "metrics_port": METRICS_PORT,
        }

        with open(
            status_file,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                status,
                f,
                indent=4
            )

    except Exception:
        pass

    # --------------------------------------------------------
    # Close RocksDB stores
    # --------------------------------------------------------

    for store in partition_stores.values():

        try:
            store.close()

        except Exception:
            pass

    partition_stores.clear()
    partition_processors.clear()

    # --------------------------------------------------------
    # Close Kafka consumer
    # --------------------------------------------------------

    consumer.close()

    print(
        f"Worker {WORKER_ID} stopped."
    )