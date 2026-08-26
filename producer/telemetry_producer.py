"""
Stream Forge - IoT / Truck Telemetry Kafka Producer

Responsibilities:
    1. Simulate IoT temperature readings from trucks.
    2. Produce telemetry events to Apache Kafka.
    3. Use truck_id as the Kafka message key.
    4. Send data to the truck_telemetry topic.
    5. Support the project's 50,000-truck / 10-second telemetry model.
    6. Use an idempotent Kafka producer for reliable delivery.
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List

from confluent_kafka import Producer


# ============================================================
# Configuration
# ============================================================

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"

# Project requirement
TOTAL_TRUCKS = 50_000
TELEMETRY_INTERVAL_SECONDS = 10

# Temperature simulation range
MIN_TEMPERATURE = 20.0
MAX_TEMPERATURE = 45.0

# Alert thresholds
HIGH_TEMPERATURE = 40.0
LOW_TEMPERATURE = 5.0

# Default demonstration size.
# Use 50000 when demonstrating the complete project requirement.
DEFAULT_DEMO_TRUCKS = 100


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("streamforge-producer")


# ============================================================
# Graceful shutdown
# ============================================================

running = True


def shutdown_handler(signum, frame):
    """
    Stop the producer gracefully when Ctrl+C or termination
    signal is received.
    """
    global running

    logger.info("Shutdown signal received.")
    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ============================================================
# Truck IoT simulator
# ============================================================

class TruckSensor:
    """
    Simulates an IoT temperature sensor installed on a truck.
    """

    def __init__(self, truck_id: int):
        self.truck_id = truck_id

        # Start each truck at a realistic temperature.
        self.temperature = random.uniform(
            MIN_TEMPERATURE,
            MAX_TEMPERATURE,
        )

    def read_temperature(self) -> float:
        """
        Generate the next temperature reading.

        A small random variation is applied so that readings
        behave more like continuous sensor data rather than
        completely independent random numbers.
        """

        change = random.uniform(-1.5, 1.5)

        self.temperature += change

        # Keep temperature inside a realistic simulation range.
        self.temperature = max(
            MIN_TEMPERATURE,
            min(MAX_TEMPERATURE, self.temperature),
        )

        return round(self.temperature, 2)

    def generate_event(self) -> Dict:
        """
        Create one telemetry event for this truck.
        """

        temperature = self.read_temperature()

        timestamp = datetime.now(timezone.utc).isoformat()

        event = {
            "event_id": str(uuid.uuid4()),
            "truck_id": self.truck_id,
            "temperature": temperature,
            "timestamp": timestamp,
        }

        # Optional status information makes the event more
        # useful for monitoring and demonstrations.
        if temperature >= HIGH_TEMPERATURE:
            event["status"] = "HIGH"
        elif temperature <= LOW_TEMPERATURE:
            event["status"] = "LOW"
        else:
            event["status"] = "NORMAL"

        return event


# ============================================================
# Kafka Producer
# ============================================================

class TruckTelemetryProducer:
    """
    Kafka producer responsible for publishing truck telemetry.
    """

    def __init__(
        self,
        broker: str = KAFKA_BROKER,
        topic: str = TOPIC,
    ):
        self.topic = topic

        producer_config = {
            "bootstrap.servers": broker,

            # Reliable delivery
            "acks": "all",

            # Enable idempotent producer behavior.
            # This prevents duplicate records caused by producer
            # retries within the Kafka producer session.
            "enable.idempotence": True,

            # Retry temporary broker/network failures.
            "retries": 10,

            # Small batching improves throughput.
            "linger.ms": 5,

            # Compression reduces network traffic.
            "compression.type": "snappy",

            # Producer queue capacity.
            "queue.buffering.max.messages": 100000,

            # Unique client identifier.
            "client.id": "streamforge-truck-producer",
        }

        self.producer = Producer(producer_config)

        self.messages_produced = 0
        self.messages_failed = 0

    # --------------------------------------------------------
    # Kafka delivery callback
    # --------------------------------------------------------

    def delivery_report(self, err, message):
        """
        Called by Kafka after a message is successfully delivered
        or fails.
        """

        if err is not None:
            self.messages_failed += 1

            logger.error(
                "Kafka delivery failed | key=%s | error=%s",
                message.key(),
                err,
            )

            return

        self.messages_produced += 1

        # Log successful delivery periodically rather than
        # printing every single message.
        if self.messages_produced % 100 == 0:
            logger.info(
                "Kafka delivery confirmed | "
                "topic=%s | partition=%s | offset=%s | total=%s",
                message.topic(),
                message.partition(),
                message.offset(),
                self.messages_produced,
            )

    # --------------------------------------------------------
    # Produce one event
    # --------------------------------------------------------

    def send(self, event: Dict):
        """
        Send a telemetry event to Kafka.

        truck_id is used as the Kafka key.

        This is important because Kafka uses the key to select
        a partition. Therefore, events belonging to the same
        truck are consistently routed to the same partition.
        """

        truck_id = event["truck_id"]

        key = str(truck_id).encode("utf-8")

        value = json.dumps(
            event,
            separators=(",", ":"),
        ).encode("utf-8")

        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self.delivery_report,
            )

            # Poll allows Kafka to execute delivery callbacks.
            self.producer.poll(0)

        except BufferError:
            logger.warning(
                "Kafka producer queue is full. "
                "Waiting for queued messages to be delivered."
            )

            self.producer.poll(1)

            # Retry this event.
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self.delivery_report,
            )

    # --------------------------------------------------------
    # Flush
    # --------------------------------------------------------

    def flush(self):
        """
        Wait until all queued Kafka messages are delivered.
        """

        logger.info("Flushing Kafka producer...")

        remaining = self.producer.flush(timeout=30)

        if remaining:
            logger.warning(
                "%s Kafka messages were not delivered.",
                remaining,
            )
        else:
            logger.info(
                "All queued Kafka messages delivered successfully."
            )


# ============================================================
# Truck fleet simulator
# ============================================================

class TruckFleetSimulator:
    """
    Simulates a fleet of IoT-enabled trucks.

    For the actual project:
        TOTAL_TRUCKS = 50,000

    For local development:
        A smaller number can be supplied through --trucks.
    """

    def __init__(
        self,
        number_of_trucks: int,
        producer: TruckTelemetryProducer,
        interval_seconds: int = TELEMETRY_INTERVAL_SECONDS,
    ):
        self.number_of_trucks = number_of_trucks
        self.producer = producer
        self.interval_seconds = interval_seconds

        self.trucks: List[TruckSensor] = [
            TruckSensor(truck_id)
            for truck_id in range(1, number_of_trucks + 1)
        ]

        self.readings_generated = 0
        self.cycles_completed = 0

    def generate_cycle(self):
        """
        Generate one telemetry reading from every truck.

        One cycle represents one IoT telemetry interval.
        """

        cycle_start = time.time()

        for truck in self.trucks:

            if not running:
                break

            event = truck.generate_event()

            self.producer.send(event)

            self.readings_generated += 1

        self.cycles_completed += 1

        elapsed = time.time() - cycle_start

        logger.info(
            "Telemetry cycle %s completed | "
            "trucks=%s | readings=%s | elapsed=%.2fs",
            self.cycles_completed,
            self.number_of_trucks,
            self.readings_generated,
            elapsed,
        )

    def run(self):
        """
        Continuously generate telemetry.

        The project requirement specifies one reading every
        10 seconds for each truck.
        """

        logger.info("=" * 70)
        logger.info("STREAM FORGE - IoT TELEMETRY PRODUCER")
        logger.info("=" * 70)
        logger.info(
            "Kafka broker       : %s",
            KAFKA_BROKER,
        )
        logger.info(
            "Kafka topic        : %s",
            TOPIC,
        )
        logger.info(
            "Simulated trucks   : %s",
            self.number_of_trucks,
        )
        logger.info(
            "Project fleet      : %s",
            TOTAL_TRUCKS,
        )
        logger.info(
            "Telemetry interval : %s seconds",
            self.interval_seconds,
        )
        logger.info(
            "Kafka partitions   : 20",
        )
        logger.info("=" * 70)

        while running:

            cycle_start = time.time()

            self.generate_cycle()

            if not running:
                break

            elapsed = time.time() - cycle_start

            sleep_time = max(
                0,
                self.interval_seconds - elapsed,
            )

            logger.info(
                "Next telemetry cycle in %.2f seconds.",
                sleep_time,
            )

            # Sleep in small intervals so Ctrl+C remains responsive.
            end_time = time.time() + sleep_time

            while running and time.time() < end_time:
                time.sleep(0.2)

        self.producer.flush()

        logger.info(
            "Producer stopped | "
            "readings_generated=%s | "
            "messages_produced=%s | "
            "messages_failed=%s",
            self.readings_generated,
            self.producer.messages_produced,
            self.producer.messages_failed,
        )


# ============================================================
# Command-line interface
# ============================================================

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Stream Forge IoT Truck Telemetry Kafka Producer"
    )

    parser.add_argument(
        "--trucks",
        type=int,
        default=DEFAULT_DEMO_TRUCKS,
        help=(
            "Number of trucks to simulate. "
            "Default: 100. "
            "Use 50000 for the full project simulation."
        ),
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=TELEMETRY_INTERVAL_SECONDS,
        help=(
            "Telemetry interval in seconds. "
            "Default: 10."
        ),
    )

    parser.add_argument(
        "--broker",
        default=KAFKA_BROKER,
        help=(
            "Kafka bootstrap server. "
            "Default: localhost:9092"
        ),
    )

    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=(
            "Kafka topic. "
            "Default: truck_telemetry"
        ),
    )

    return parser.parse_args()


# ============================================================
# Main
# ============================================================

def main():
    args = parse_arguments()

    if args.trucks <= 0:
        logger.error("Number of trucks must be greater than zero.")
        sys.exit(1)

    if args.interval <= 0:
        logger.error("Interval must be greater than zero.")
        sys.exit(1)

    producer = TruckTelemetryProducer(
        broker=args.broker,
        topic=args.topic,
    )

    simulator = TruckFleetSimulator(
        number_of_trucks=args.trucks,
        producer=producer,
        interval_seconds=args.interval,
    )

    try:
        simulator.run()

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")

    except Exception:
        logger.exception("Fatal producer error.")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()


   