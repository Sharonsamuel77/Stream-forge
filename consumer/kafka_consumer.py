from confluent_kafka import Consumer, KafkaException, KafkaError
import json


# --------------------------------
# Kafka configuration
# --------------------------------

KAFKA_BROKER = "localhost:9092"
TOPIC = "truck_telemetry"
GROUP_ID = "streamforge-consumer-group"


# --------------------------------
# Consumer configuration
# --------------------------------

consumer_config = {
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}


consumer = Consumer(consumer_config)


# --------------------------------
# Partition assignment callback
# --------------------------------

def on_assign(consumer, partitions):
    print("\n===================================")
    print("Partitions Assigned")
    print("===================================")

    for partition in partitions:
        print(
            f"Topic: {partition.topic} | "
            f"Partition: {partition.partition} | "
            f"Offset: {partition.offset}"
        )

    consumer.assign(partitions)


# --------------------------------
# Partition revoke callback
# --------------------------------

def on_revoke(consumer, partitions):
    print("\n===================================")
    print("Partitions Revoked")
    print("===================================")

    for partition in partitions:
        print(
            f"Topic: {partition.topic} | "
            f"Partition: {partition.partition}"
        )


# --------------------------------
# Subscribe to Kafka topic
# --------------------------------

consumer.subscribe(
    [TOPIC],
    on_assign=on_assign,
    on_revoke=on_revoke
)


print("===================================")
print("      StreamForge Kafka Consumer")
print("===================================")
print(f"Broker      : {KAFKA_BROKER}")
print(f"Topic       : {TOPIC}")
print(f"Consumer ID : {GROUP_ID}")
print("Waiting for messages...")
print("Press Ctrl+C to stop")
print()


# --------------------------------
# Consume messages
# --------------------------------

try:

    while True:

        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():

            if message.error().code() == KafkaError._PARTITION_EOF:
                print(
                    f"Reached end of partition: "
                    f"{message.partition()}"
                )
                continue

            raise KafkaException(message.error())

        try:

            event = json.loads(
                message.value().decode("utf-8")
            )

            print(
                f"Received | "
                f"truck={event['truck_id']} | "
                f"temperature={event['temperature']} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )

        except (json.JSONDecodeError, KeyError) as error:

            print(f"Invalid event received: {error}")


except KeyboardInterrupt:

    print("\nStopping consumer...")


finally:

    consumer.close()

    print("Consumer stopped.")