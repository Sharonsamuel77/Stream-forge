# StreamForge

## Distributed Python Event Processor

StreamForge is a distributed real-time event processing system built using **Apache Kafka, Python, RocksDB, Prometheus, FastAPI, and React**.

The project simulates an IoT truck fleet where vehicles continuously send temperature telemetry. Kafka distributes the incoming events across multiple partitions, Python workers process the events in parallel, RocksDB maintains persistent state, Prometheus monitors the system, and a React dashboard provides a real-time view of the system.

---

## Project Architecture

```text
                 IoT Truck Telemetry
                         |
                         v
              +---------------------+
              |    Kafka Producer    |
              +---------------------+
                         |
                         v
              +---------------------+
              |    Apache Kafka     |
              |  truck_telemetry    |
              |    20 Partitions    |
              +---------------------+
                         |
             +-----------+-----------+
             |           |           |
             v           v           v
         Worker 1    Worker 2    Worker N
             |           |           |
             +-----------+-----------+
                         |
                         v
              +---------------------+
              |   Event Processing  |
              | Temperature Average |
              +---------------------+
                         |
                         v
              +---------------------+
              |       RocksDB       |
              |   Persistent State  |
              +---------------------+

        +-----------------------------+
        |     Prometheus Monitoring   |
        | Throughput / Lag / Workers |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |       FastAPI Backend       |
        +-----------------------------+
                       |
                       v
        +-----------------------------+
        |      React Dashboard        |
        | Topology / Metrics / Alerts |
        +-----------------------------+
```

---

## Project Flow

1. The producer generates truck temperature telemetry.
2. Telemetry is published to the Kafka topic `truck_telemetry`.
3. Kafka distributes events across **20 partitions**.
4. Multiple Python workers consume the partitions through a Kafka consumer group.
5. Workers process the incoming temperature events in parallel.
6. The system calculates truck-wise temperature state, including a **5-minute rolling average**.
7. RocksDB stores processing state for persistence and recovery.
8. Kafka offsets are manually committed after processing.
9. Prometheus collects monitoring metrics such as worker status, throughput, and Kafka lag.
10. FastAPI provides monitoring and system-state APIs.
11. The React dashboard displays the live system status, workers, partitions, readings, throughput, alerts, and topology.

---

## Example Telemetry Event

```json
{
  "truck_id": "truck_1",
  "temperature": 32.5,
  "timestamp": "2026-08-14T20:00:00"
}
```

---

## Main Components

### 1. Kafka Producer

Generates simulated truck telemetry and publishes it to:

```text
truck_telemetry
```

### 2. Apache Kafka

Kafka acts as the distributed messaging layer.

The project uses:

* 20 Kafka partitions
* Kafka consumer groups
* Dynamic partition assignment
* Consumer-group rebalancing

### 3. Python Workers

Multiple workers process Kafka partitions in parallel.

Workers handle:

* Telemetry consumption
* Temperature processing
* State updates
* Offset commits
* Worker-level metrics

### 4. RocksDB

RocksDB is used for persistent state storage.

It allows the system to retain truck processing state and recover state when workers restart.

### 5. Prometheus

Prometheus is used to monitor the distributed processing system.

The monitoring layer tracks information such as:

* Processed events
* Consumed events
* Processing errors
* Worker activity
* Throughput
* Kafka lag
* Active trucks
* System health

The StreamForge metrics server exposes Prometheus metrics at:

```text
http://localhost:9000/metrics
```

### 6. FastAPI

FastAPI provides the backend APIs used by the dashboard.

The backend provides information related to:

* System summary
* Worker status
* Kafka partitions
* Processing state
* Alerts
* Recovery status
* Stream topology
* WebSocket live metrics

### 7. React Dashboard

The React frontend provides a real-time monitoring interface.

The dashboard displays:

* Total trucks
* Active workers
* Total readings
* Average temperature
* Throughput
* Kafka lag
* Number of partitions
* Active alerts
* Worker health
* Stream topology
* RocksDB recovery status

---

## Technologies Used

| Technology      | Purpose                       |
| --------------- | ----------------------------- |
| Python          | Event processing and workers  |
| Apache Kafka    | Distributed event streaming   |
| Confluent Kafka | Python Kafka client           |
| RocksDB         | Persistent state storage      |
| Prometheus      | Monitoring and metrics        |
| FastAPI         | Backend monitoring API        |
| WebSocket       | Live metric updates           |
| React           | Frontend dashboard            |
| React Flow      | Stream topology visualization |
| Docker          | Kafka and Prometheus services |

---

## Requirements

Install the following:

* Python 3.11+
* Git
* Docker Desktop
* Node.js LTS
* npm

Python dependencies are listed in `requirements.txt`.

---

## Running StreamForge

### 1. Start Docker Services

```powershell
docker compose up -d
```

### 2. Start the Metrics Server

```powershell
python monitoring\metrics_server.py
```

Metrics are available at:

```text
http://localhost:9000/metrics
```

### 3. Start StreamForge

```powershell
python run.py
```

### 4. Start FastAPI

```powershell
python -m uvicorn dashboard.backend.main:app --reload --port 8001
```

The backend runs at:

```text
http://127.0.0.1:8001
```

### 5. Start the React Dashboard

Navigate to:

```text
dashboard\frontend
```

Then run:

```powershell
npm.cmd run dev
```

The dashboard runs at:

```text
http://localhost:5173
```

---

## Monitoring

| Service         | Address                         |
| --------------- | ------------------------------- |
| Kafka           | `localhost:9092`                |
| Prometheus      | `http://localhost:9090`         |
| Metrics Server  | `http://localhost:9000/metrics` |
| FastAPI         | `http://127.0.0.1:8001`         |
| React Dashboard | `http://localhost:5173`         |

---

## Final Verification

The StreamForge system was tested end-to-end with the following components working together:

* Kafka broker
* 20 Kafka partitions
* Multiple Python workers
* Dynamic partition assignment
* Consumer-group rebalancing
* Worker processing
* 5-minute temperature processing
* RocksDB state persistence
* Kafka offset commits
* Prometheus monitoring
* FastAPI monitoring APIs
* WebSocket metrics
* React dashboard
* React Flow stream topology
* Throughput monitoring
* Kafka lag monitoring
* Worker health monitoring
* End-to-end telemetry processing

The final integration demonstrated the complete flow from telemetry generation through Kafka, distributed worker processing, state persistence, monitoring, and dashboard visualization.

---

## Team Contributions

The project was developed as a team with separate modules for:

* Kafka Producer and ingestion
* Worker processing
* RocksDB state and recovery
* Prometheus monitoring
* FastAPI and React dashboard

### Monitoring Contribution

The monitoring module focused on:

* Prometheus integration
* Metrics server
* Prometheus scrape configuration
* Worker and processing metrics
* Throughput monitoring
* Kafka lag monitoring
* Integration of monitoring data with the dashboard
