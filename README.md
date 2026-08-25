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
