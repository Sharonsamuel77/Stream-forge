from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time


# Total messages consumed
messages_consumed_total = Counter(
    "streamforge_messages_consumed_total",
    "Total number of Kafka messages consumed"
)

# Total messages successfully processed
messages_processed_total = Counter(
    "streamforge_messages_processed_total",
    "Total number of Kafka messages successfully processed"
)

# Total processing errors
processing_errors_total = Counter(
    "streamforge_processing_errors_total",
    "Total number of message processing errors"
)

# Number of active trucks
active_trucks = Gauge(
    "streamforge_active_trucks",
    "Number of active trucks"
)

# Message processing latency
processing_latency_seconds = Histogram(
    "streamforge_processing_latency_seconds",
    "Time taken to process a Kafka message"
)

# Worker health
worker_up = Gauge(
    "streamforge_worker_up",
    "Worker health status: 1 = running, 0 = stopped"
)


def start_metrics_server():
    """Start the Prometheus metrics HTTP server."""
    start_http_server(9000)

    worker_up.set(1)

    print("===================================")
    print("   StreamForge Metrics Server")
    print("===================================")
    print("Prometheus metrics available at:")
    print("http://localhost:8000/metrics")
    print("Press Ctrl+C to stop")


if __name__ == "__main__":
    start_metrics_server()

    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        worker_up.set(0)
        print("\nMetrics server stopped.")