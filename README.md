# Stream-forge
Distributed Python Event Processor.

## Kafka Worker Partition Assignment

StreamForge uses Apache Kafka consumer groups to distribute telemetry partitions dynamically among Python workers.

### Architecture

```text
                    Kafka Broker
                        |
                truck_telemetry
                        |
              20 Kafka Partitions
        ┌────┬────┬────┬────┬───────┐
        P0   P1   P2   P3   ...     P19
         \    \    \    \            /
          \    \    \    \          /
           └──── Consumer Group ────┘
              streamforge-state-workers
                        |
              ┌─────────┴─────────┐
              │                   │
           Worker 1            Worker 2
           Worker 3            Worker N

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
