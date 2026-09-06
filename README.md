# Stream-forge
Distributed Python Event Processor.


**🏗️ Architecture**

**Kafka Worker Partition Assignment**

StreamForge uses Apache Kafka consumer groups to distribute telemetry partitions dynamically among Python workers.

                    ┌──────────────────┐
                    │   Kafka Broker   │
                    └────────┬─────────┘
                             │
                             ▼
                    truck_telemetry
                             │
                             ▼
                    20 Kafka Partitions
                             │
       ┌────┬────┬────┬─────┼─────┬────┐
       │ P0 │ P1 │ P2 │ P3  │ ... │ P19│
       └────┴────┴────┴─────┼─────┴────┘
                             │
                             ▼
                  ┌──────────────────────┐
                  │    Consumer Group    │
                  │                      │
                  │ streamforge-state-   │
                  │ workers              │
                  └──────────┬───────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         ┌─────────┐    ┌─────────┐    ┌─────────┐
         │ Worker 1│    │ Worker 2│    │ Worker N│
         └─────────┘    └─────────┘    └─────────┘

Kafka automatically assigns partitions to available workers.

If a worker joins or leaves the consumer group, Kafka performs a rebalance and redistributes the partitions.

## Requirements

Before running StreamForge, install the following software.

### Software to Download

| Software | Recommended Version | Purpose |
|---|---|---|
| Python | 3.11.x | Backend, workers and stream processing |
| Git | Latest | Clone and manage the repository |
| Docker Desktop | Latest | Run Apache Kafka |
| WSL 2 | Latest | Required/recommended for Docker Desktop on Windows |
| Node.js | LTS | Run the React frontend |
| npm | Included with Node.js | Install frontend dependencies |

### Python

Install Python 3.11.x and make sure Python is added to PATH.

Verify the installation:

```powershell
python --version

# Stream Forge

## Distributed Python Event Processor

Stream Forge is a distributed real-time event processing system built using **Apache Kafka, Python, FastAPI, RocksDB, Prometheus, and React**.

The system is designed to process high-volume IoT telemetry data from trucks. Incoming telemetry events are published to Kafka, distributed across Kafka partitions, and automatically assigned to multiple Python worker processes.

Each worker processes its assigned partitions and maintains state using RocksDB. The system also provides a FastAPI monitoring API and a React-based dashboard for monitoring workers, partitions, throughput, lag, and system topology.

---

## Project Overview

The main use case is an IoT fleet monitoring system where thousands of trucks continuously send temperature telemetry.

Example event:

```json
{
  "truck_id": 1001,
  "temperature": 32.5,
  "timestamp": "2026-08-14T20:00:00"
}


### Verification Summary

The system successfully demonstrates distributed event processing using Apache Kafka, with dynamic partition assignment across multiple Python workers, persistent state management using RocksDB, real-time monitoring through FastAPI and WebSockets, and visualization through the React dashboard.

Worker failures were also tested to verify that Kafka automatically rebalances partitions among the remaining workers.

**🧪 End-to-End Verification**

The complete system has been tested through the following workflow:
Docker
  ↓
Kafka Broker
  ↓
truck_telemetry
  ↓
20 Kafka Partitions
  ↓
Consumer Group
  ↓
Multiple Python Workers
  ↓
Telemetry Processing
  ↓
5-Minute Rolling Average
  ↓
RocksDB State
  ↓
Kafka Offset Commit
  ↓
FastAPI / WebSockets
  ↓
Prometheus
  ↓
React Dashboard

✅ Final Verification Status

The StreamForge system has been tested end-to-end.

| Component                               | Status    |
| --------------------------------------- | --------- |
| Kafka Broker                            | ✅ Working |
| 20 Kafka Partitions                     | ✅ Working |
| Multiple Python Workers                 | ✅ Working |
| Dynamic Partition Assignment            | ✅ Working |
| Consumer-Group Rebalancing              | ✅ Working |
| Worker Failure & Recovery               | ✅ Working |
| Telemetry Ingestion                     | ✅ Working |
| 5-Minute Rolling Temperature Processing | ✅ Working |
| RocksDB State Persistence               | ✅ Working |
| Kafka Offset Commits                    | ✅ Working |
| FastAPI Monitoring                      | ✅ Working |
| WebSocket Metrics                       | ✅ Working |
| React Topology Dashboard                | ✅ Working |
| Throughput & Metrics Display            | ✅ Working |
| End-to-End Event Processing             | ✅ Working |
