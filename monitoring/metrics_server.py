from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)
import time


# ============================================================
# Metrics
# ============================================================

messages_consumed_total = Counter(
    "streamforge_messages_consumed_total",
    "Total number of Kafka messages consumed"
)

messages_processed_total = Counter(
    "streamforge_messages_processed_total",
    "Total number of Kafka messages successfully processed"
)

processing_errors_total = Counter(
    "streamforge_processing_errors_total",
    "Total number of message processing errors"
)

active_trucks = Gauge(
    "streamforge_active_trucks",
    "Number of active trucks"
)

processing_latency_seconds = Histogram(
    "streamforge_processing_latency_seconds",
    "Time taken to process a Kafka message"
)

worker_up = Gauge(
    "streamforge_worker_up",
    "Worker health status: 1 = running, 0 = stopped"
)


# ============================================================
# Metrics Server
# ============================================================

def start_metrics_server(port=8000):
    """Start the Prometheus metrics HTTP server."""

    start_http_server(port)

    worker_up.set(1)

    print("===================================")
    print("   StreamForge Metrics Server")
    print("===================================")

    print(
        "Prometheus metrics available at:"
    )

    print(
        f"http://localhost:{port}/metrics"
    )

    print(
        "Press Ctrl+C to stop"
    )

# ============================================================
# Standalone execution
# ============================================================

if __name__ == "__main__":

    start_metrics_server()

    try:

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        worker_up.set(0)

        print(
            "\nMetrics server stopped."
        )