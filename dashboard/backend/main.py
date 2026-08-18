from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
#from state.rocksdb_store import RocksDBStateStore
import requests
import re
from state.rocksdb_store import RocksDBStateStore
import json
import os

app = FastAPI()

# --------------------------------------------------
# CORS
# --------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------
# Throughput History
# --------------------------------------------------

metrics_history = []

# --------------------------------------------------
# Home Endpoint
# --------------------------------------------------

@app.get("/")
def home():
    return {
        "message": "StreamForge Dashboard API"
    }

# --------------------------------------------------
# Live Metrics from Prometheus
# --------------------------------------------------
@app.get("/state")
def get_state():

    path = "data/state_snapshot.json"

    if not os.path.exists(path):
        return []

    try:
        with open("data/state_snapshot.json", "r") as f:
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

@app.get("/metrics")
def metrics():

    response = requests.get(
        "http://localhost:9000/metrics",
        timeout=5
    )

    text = response.text

    consumed = re.search(
        r"streamforge_messages_consumed_total\s+([0-9.]+)",
        text
    )

    errors = re.search(
        r"streamforge_processing_errors_total\s+([0-9.]+)",
        text
    )

    trucks = re.search(
        r"streamforge_active_trucks\s+([0-9.]+)",
        text
    )

    current_time = datetime.now().strftime("%H:%M:%S")

    current_throughput = (
        int(float(consumed.group(1)))
        if consumed else 0
    )

    metrics_history.append({
        "time": current_time,
        "throughput": current_throughput
    })

    # Keep only last 20 points
    metrics_history[:] = metrics_history[-20:]

    return {
        "throughput": current_throughput,
        "active_workers": int(float(trucks.group(1))) if trucks else 0,
        "failed_events": int(float(errors.group(1))) if errors else 0,
    }

# --------------------------------------------------
# Throughput History
# --------------------------------------------------

@app.get("/metrics/history")
def metrics_history_endpoint():
    return metrics_history

# --------------------------------------------------
# Worker Information
# --------------------------------------------------

@app.get("/workers")
def workers():

    try:
        with open("data/rocksdb/worker_status.json", "r") as f:
            status = json.load(f)
    except Exception:
        return []

    workers = []

    for worker_id, info in status.items():
        workers.append({
            "id": worker_id,
            "status": info["status"],
            "partition": f"partition{info['partition']}",
            "last_seen": info["last_seen"]
        })

    return workers

@app.get("/summary")
def summary():

    try:
        with open("data/state_snapshot.json", "r") as f:
            data = json.load(f)
    except Exception:
        return {
        "total_trucks": 0,
        "total_readings": 0,
        "avg_temperature": 0
    }

    total_trucks = len(data)

    total_readings = sum(
        truck["readings"]
        for truck in data.values()
    )

    avg_temp = round(
        sum(
            truck["avg_temperature"]
            for truck in data.values()
        ) / total_trucks,
        2
    )
    alerts_count = 0

    for truck in data.values():
        if truck["avg_temperature"] > 32:
            alerts_count += 1

    active_workers = 0

    try:
        with open("data/worker_status.json", "r") as f:
            workers = json.load(f)

        active_workers = len(workers)

    except Exception:
        active_workers = 0

    return {
        "total_trucks": total_trucks,
        "active_workers": active_workers,
        "total_readings": total_readings,
        "avg_temperature": avg_temp,
        "active_alerts":alerts_count
    }

@app.get("/alerts")
def alerts():

    path = "data/state_snapshot.json"

    if not os.path.exists(path):
        return {
            "count": 0,
            "alerts": []
        }

    try:
        with open(path, "r") as f:
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
            "temperature": round(temp, 2),
            "severity": severity
        })

    return {
        "count": len(alert_list),
        "alerts": alert_list
    }

@app.get("/recovery")
def recovery():
    return {
        "status": "Recovered",
        "state_store": "RocksDB",
        "records": 10
    }