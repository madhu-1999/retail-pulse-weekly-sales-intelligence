import json
import signal
import sys
from pathlib import Path

from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_GROUP, KAFKA_TOPIC
from confluent_kafka import Consumer, KafkaError, KafkaException
from logging_config import get_logger

logger = get_logger("kafka_consumer", "consumer.log")

_running = True

OUTPUT_FILE = Path(__file__).resolve().parent / "consumed_events.jsonl"


def _shutdown(signum, frame) -> None:
    global _running
    logger.info("Shutdown signal received (%s), stopping consumer...", signum)
    _running = False


def main() -> None:
    if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP]):
        logger.error(
            "Missing Kafka connection settings. "
            "Check KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC / "
            "KAFKA_CONSUMER_GROUP in your .env file."
        )
        sys.exit(1)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    consumer.subscribe([KAFKA_TOPIC])  # type: ignore
    logger.info(
        f"Subscribed to topic '{KAFKA_TOPIC}' as group '{KAFKA_CONSUMER_GROUP}'. Waiting for events (Ctrl+C to stop)..."
    )
    output_fh = open(OUTPUT_FILE, "a", encoding="utf-8")

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)  # 1 s timeout to fetch new events
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:  # type: ignore
                    logger.info(
                        "Reached end of partition %s[%d]", msg.topic(), msg.partition()
                    )
                    continue
                logger.error("Consumer error: %s", msg.error())
                raise KafkaException(msg.error())

            try:
                event = json.loads(msg.value().decode("utf-8"))  # type: ignore
            except json.JSONDecodeError:
                logger.error(
                    "Skipping malformed message at %s[%d]@%d",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                continue

            logger.info(
                f"Consumed event_id={event.get('event_id')} category={event.get('category')} sales_month={event.get('sales_month')} orders={event.get('order_count')} quantity={event.get('total_quantity')} revenue={event.get('total_revenue')} [partition={msg.partition()} offset={msg.offset()}]"
            )

            # Print every message as it arrives.
            print(json.dumps(event))

            # Persist every message, one JSON object per line.
            output_fh.write(json.dumps(event) + "\n")
            output_fh.flush()
    finally:
        logger.info("Closing consumer...")
        output_fh.close()
        consumer.close()


if __name__ == "__main__":
    main()
