from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import asyncio
import requests
import re
from state.rocksdb_store import RocksDBStateStore
import json
import os
import time
from confluent_kafka.admin import AdminClient

app = FastAPI()


# ============================================================
# CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# Throughput History
# ============================================================

metrics_history = []

throughput_samples = []

latest_throughput = 0.0


# ============================================================
# Home Endpoint
# ============================================================

@app.get("/")
def home():

    return {
        "message": "StreamForge Dashboard API"
    }


# ============================================================
# State
# ============================================================

@app.get("/state")
def get_state():

    path = "data/state_snapshot.json"

    if not os.path.exists(path):
        return []

    try:

        with open(path, "r") as f:
            data = json.load(f)

    except Exception:

        return []

    trucks = []

    for truck_id, info in data.items():

        trucks.append({

            "truck_id": truck_id,

            "avg_temperature": info["avg_temperature"],

            "readings": info["readings"]

        })

    return trucks


# ============================================================
# Live Metrics
# ============================================================

# ============================================================
# Live Metrics
# ============================================================

@app.get("/metrics")
def metrics():

    global latest_throughput

    # --------------------------------------------------------
    # Collect metrics from all worker metric servers
    # --------------------------------------------------------

    worker_metrics = []

    for port in range(8101, 8121):

        try:

            response = requests.get(
                f"http://127.0.0.1:{port}/metrics",
                timeout=0.2
            )

            response.raise_for_status()

            worker_metrics.append(response.text)

        except Exception:

            continue

    # --------------------------------------------------------
    # No workers available
    # --------------------------------------------------------

    if not worker_metrics:

        return {
            "throughput": 0,
            "active_workers": 0,
            "failed_events": 0,
            "partitions": get_partition_count(),
            "total_lag": get_total_lag(),
            "error": "No worker metrics available"
        }

    # Combine metrics from all workers
    text = "\n".join(worker_metrics)

    # --------------------------------------------------------
    # Extract metrics from ALL workers
    # --------------------------------------------------------

    consumed_values = re.findall(
        r"streamforge_messages_consumed_total\s+([0-9.]+)",
        text
    )

    processed_values = re.findall(
        r"streamforge_messages_processed_total\s+([0-9.]+)",
        text
    )

    error_values = re.findall(
        r"streamforge_processing_errors_total\s+([0-9.]+)",
        text
    )

    worker_up_values = re.findall(
        r"streamforge_worker_up\s+([0-9.]+)",
        text
    )

    # --------------------------------------------------------
    # Aggregate worker metrics
    # --------------------------------------------------------

    consumed_value = sum(
        float(value)
        for value in consumed_values
    )

    processed_value = sum(
        float(value)
        for value in processed_values
    )

    failed_value = int(
        sum(
            float(value)
            for value in error_values
        )
    )

    active_workers = int(
        sum(
            float(value)
            for value in worker_up_values
        )
    )
    print(
        "DEBUG METRICS:",
        len(worker_metrics),
        "workers_found:",
        worker_up_values,
        "processed:",
        processed_values,
        flush=True
    )
    # --------------------------------------------------------
    # Calculate throughput
    # --------------------------------------------------------

    current_time_seconds = time.time()

    throughput_samples.append(
        (
            current_time_seconds,
            processed_value
        )
    )

    # Keep only samples from the last 10 seconds
    cutoff_time = current_time_seconds - 10

    throughput_samples[:] = [
        sample
        for sample in throughput_samples
        if sample[0] >= cutoff_time
    ]

    current_throughput = 0.0

    if len(throughput_samples) >= 2:

        oldest_time, oldest_processed = throughput_samples[0]

        newest_time, newest_processed = throughput_samples[-1]

        elapsed = newest_time - oldest_time

        processed_difference = (
            newest_processed - oldest_processed
        )

        if elapsed > 0 and processed_difference > 0:

            current_throughput = round(
                processed_difference / elapsed,
                2
            )

    if current_throughput == 0.0 and len(throughput_samples) >= 2:

        previous_time, previous_processed = throughput_samples[-2]

        elapsed = current_time_seconds - previous_time

        processed_difference = (
            processed_value - previous_processed
        )

        if elapsed > 0 and processed_difference > 0:

            current_throughput = round(
                processed_difference / elapsed,
                2
            )

    latest_throughput = current_throughput

    # --------------------------------------------------------
    # History
    # --------------------------------------------------------

    current_time = datetime.now().strftime(
        "%H:%M:%S"
    )

    metrics_history.append({

        "time": current_time,

        "throughput": current_throughput,

        "processed": int(
            processed_value
        ),

        "consumed": int(
            consumed_value
        )

    })

    # Keep only last 20 points
    metrics_history[:] = metrics_history[-20:]

    # --------------------------------------------------------
    # Return dashboard metrics
    # --------------------------------------------------------

    return {

        "throughput": current_throughput,

        "active_workers": active_workers,

        "failed_events": failed_value,

        "partitions": get_partition_count(),

        "total_lag": get_total_lag()

    }

@app.get("/metrics/history")
def metrics_history_endpoint():

    return metrics_history


# ============================================================
# Worker Information
# ============================================================

@app.get("/workers")
def workers():

    try:

        with open(

            "data/rocksdb/worker_status.json",

            "r"

        ) as f:

            status = json.load(f)

    except Exception:

        return []


    workers = []


    for worker_id, info in status.items():

        workers.append({

            "id": worker_id,

            "status": info["status"],

            "last_seen": info["last_seen"]

        })


    return workers


# ============================================================
# Summary
# ============================================================

@app.get("/summary")
def summary():

    try:

        with open(

            "data/state_snapshot.json",

            "r"

        ) as f:

            data = json.load(f)

    except Exception:

        return {

            "total_trucks": 0,

            "active_workers": 0,

            "total_readings": 0,

            "avg_temperature": 0,

            "active_alerts": 0

        }


    # --------------------------------------------------------
    # Truck count
    # --------------------------------------------------------

    total_trucks = len(data)


    # --------------------------------------------------------
    # Total readings
    # --------------------------------------------------------

    total_readings = sum(

        truck["readings"]

        for truck in data.values()

    )


    # --------------------------------------------------------
    # Average temperature
    # --------------------------------------------------------

    if total_trucks > 0:

        avg_temp = round(

            sum(

                truck["avg_temperature"]

                for truck in data.values()

            ) / total_trucks,

            2

        )

    else:

        avg_temp = 0


    # --------------------------------------------------------
    # Alerts
    # --------------------------------------------------------

    alerts_count = 0


    for truck in data.values():

        if truck["avg_temperature"] > 32:

            alerts_count += 1


    # --------------------------------------------------------
    # Active workers
    # --------------------------------------------------------

    active_workers = 0


    try:

        with open(

            "data/rocksdb/worker_status.json",

            "r"

        ) as f:

            worker_data = json.load(f)


        active_workers = sum(

            1

            for worker in worker_data.values()

            if worker["status"] == "active"

        )

    except Exception:

        active_workers = 0


    # --------------------------------------------------------
    # Return summary
    # --------------------------------------------------------

    return {

        "total_trucks": total_trucks,

        "active_workers": active_workers,

        "total_readings": total_readings,

        "avg_temperature": avg_temp,

        "active_alerts": alerts_count

    }


# ============================================================
# Alerts
# ============================================================

@app.get("/alerts")
def alerts():

    path = "data/state_snapshot.json"


    if not os.path.exists(path):

        return {

            "count": 0,

            "alerts": []

        }


    try:

        with open(

            path,

            "r"

        ) as f:

            data = json.load(f)

    except Exception:

        return {

            "count": 0,

            "alerts": []

        }


    alert_list = []


    for truck_id, info in data.items():

        temp = info["avg_temperature"]


        if temp > 40:

            severity = "CRITICAL"

        elif temp > 35:

            severity = "HIGH"

        elif temp > 32:

            severity = "MEDIUM"

        else:

            continue


        alert_list.append({

            "truck_id": truck_id,

            "temperature": round(

                temp,

                2

            ),

            "severity": severity

        })


    return {

        "count": len(alert_list),

        "alerts": alert_list

    }


# ============================================================
# Recovery
# ============================================================

@app.get("/recovery")
def recovery():

    records = 0


    try:

        with open(

            "data/state_snapshot.json",

            "r"

        ) as f:

            data = json.load(f)


        records = len(data)


    except Exception:

        records = 0


    return {

        "status": "Recovered",

        "state_store": "RocksDB",

        "records": records

    }


# ============================================================
# Stream Topology
# ============================================================

@app.get("/api/topology")
def topology():

    return {

        "nodes": [

            {

                "id": "producer",

                "type": "default",

                "position": {
                    "x": 0,
                    "y": 150
                },

                "data": {
                    "label": "Telemetry Producer"
                }

            },

            {

                "id": "kafka",

                "type": "default",

                "position": {
                    "x": 300,
                    "y": 150
                },

                "data": {
                    "label": "Kafka"
                }

            },

            {

                "id": "worker",

                "type": "default",

                "position": {
                    "x": 600,
                    "y": 150
                },

                "data": {
                    "label": "StreamForge Worker"
                }

            },

            {

                "id": "rocksdb",

                "type": "default",

                "position": {
                    "x": 900,
                    "y": 150
                },

                "data": {
                    "label": "RocksDB"
                }

            }

        ],

        "edges": [

            {

                "id": "producer-kafka",

                "source": "producer",

                "target": "kafka",

                "animated": True

            },

            {

                "id": "kafka-worker",

                "source": "kafka",

                "target": "worker",

                "animated": True

            },

            {

                "id": "worker-rocksdb",

                "source": "worker",

                "target": "rocksdb",

                "animated": True

            }

        ]

    }


# ============================================================
# WebSocket Metrics
# ============================================================

@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):

    await websocket.accept()


    try:

        while True:

            try:

                throughput = float(
                    latest_throughput
                )


                await websocket.send_json({

                    "throughput": round(
                        throughput,
                        2
                    ),

                    "total_events_sec": round(
                        throughput,
                        2
                    )

                })


            except Exception as e:

                print(
                    "WebSocket metrics error:",
                    e
                )


                await websocket.send_json({

                    "throughput": 0,

                    "total_events_sec": 0

                })


            await asyncio.sleep(2)


    except Exception:

        pass


# ============================================================
# Kafka Configuration
# ============================================================

KAFKA_BOOTSTRAP = "localhost:9092"

KAFKA_TOPIC = "truck_telemetry"


kafka_admin = AdminClient({

    "bootstrap.servers":
        KAFKA_BOOTSTRAP

})


# ============================================================
# Kafka Partition Count
# ============================================================

def get_partition_count():

    try:

        metadata = kafka_admin.list_topics(

            topic=KAFKA_TOPIC,

            timeout=5

        )


        topic = metadata.topics.get(
            KAFKA_TOPIC
        )


        if topic is None:

            return 0


        return len(
            topic.partitions
        )


    except Exception as e:

        print(
            "Kafka partition error:",
            e
        )

        return 0


# ============================================================
# Kafka Total Lag
# ============================================================

def get_total_lag():

    try:

        from confluent_kafka import (
            TopicPartition
        )

        from confluent_kafka.admin import (
            OffsetSpec,
            _ConsumerGroupTopicPartitions
        )


        partitions = [

            TopicPartition(
                KAFKA_TOPIC,
                p
            )

            for p in range(
                get_partition_count()
            )

        ]


        request = _ConsumerGroupTopicPartitions(

            "streamforge-state-workers",

            partitions

        )


        response = (
            kafka_admin
            .list_consumer_group_offsets(
                [request]
            )
        )


        committed = (
            response[
                "streamforge-state-workers"
            ].result()
        )


        offset_requests = {}


        for tp in committed.topic_partitions:

            if (

                tp.offset is not None

                and tp.offset >= 0

            ):

                offset_requests[tp] = (
                    OffsetSpec.latest()
                )


        if not offset_requests:

            return 0


        latest_response = (
            kafka_admin.list_offsets(
                offset_requests
            )
        )


        total_lag = 0


        for tp in committed.topic_partitions:

            committed_offset = tp.offset


            if (

                committed_offset is None

                or committed_offset < 0

            ):

                continue


            latest = (
                latest_response[tp]
                .result()
                .offset
            )


            total_lag += max(

                0,

                latest - committed_offset

            )


        return total_lag


    except Exception as e:

        print(
            "Kafka lag error:",
            e
        )

        return 0