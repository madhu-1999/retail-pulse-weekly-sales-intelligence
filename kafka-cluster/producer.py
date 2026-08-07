import json
import os
from pathlib import Path
import sys

from config import (
    DATABRICKS_ACCESS_TOKEN,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_SERVER_HOSTNAME,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
)
from confluent_kafka import Producer
from databricks import sql
from logging_config import get_logger

logger = get_logger("kafka_producer", "producer.log")

QUERY = """
SELECT
    category,
    order_year,
    order_month,
    total_monthly_orders,
    total_monthly_products_sold,
    monthly_net_sales
FROM retail_pulse.retail_gold.monthly_category_sales
"""

CONSUMED_EVENTS_FILE = Path(__file__).resolve().parent / "consumed_events.jsonl"


def build_event(row) -> dict:
    sales_month = f"{row.order_year:04d}-{row.order_month:02d}"
    return {
        "event_id": f"SALES-{sales_month}-{row.category}",
        "event_type": "MONTHLY_CATEGORY_SALES_READY",
        "sales_month": sales_month,
        "category": row.category,
        "order_count": row.total_monthly_orders,
        "total_quantity": row.total_monthly_products_sold,
        "total_revenue": float(row.monthly_net_sales),
    }


def delivery_report(err, msg) -> None:
    """Called by kafka for every produced message (success or failure)."""
    if err is not None:
        logger.error(f"Delivery failed for key={msg.key()}: {err}")
    else:
        logger.info(
            f"Delivered key={msg.key()} to {msg.topic()}[{msg.partition()}]{msg.offset()}"
        )


def fetch_rows():
    if not all(
        [DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN]
    ):
        logger.error(
            "Missing Databricks connection settings. "
            "Check DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / "
            "DATABRICKS_ACCESS_TOKEN in your .env file."
        )
        sys.exit(1)

    logger.info("Connecting to Databricks SQL warehouse...")
    with (
        sql.connect(
            server_hostname=DATABRICKS_SERVER_HOSTNAME,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_ACCESS_TOKEN,
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute(QUERY)
        rows = cursor.fetchall()

    logger.info(f"Fetched {len(rows)} rows from retail_gold.monthly_category_sales")
    return rows


def load_seen_keys_from_file(file_path: Path) -> set:
    """Reads consumed_events.jsonl to extract existing event_ids."""
    seen_keys = set()
    if not os.path.exists(file_path):
        logger.info(
            f"No existing '{file_path}' file found. Starting with empty seen_keys."
        )
        return seen_keys

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    event_id = event.get("event_id")
                    if event_id:
                        seen_keys.add(event_id)
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse line {line_number} in {file_path}")
        logger.info(f"Loaded {len(seen_keys)} unique event keys from '{file_path}'.")
    except Exception as e:
        logger.error(f"Error reading '{file_path}': {e}")

    return seen_keys


def main() -> None:
    if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP]):
        logger.error(
            "Missing Kafka connection settings. "
            "Check KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC / "
            "KAFKA_CONSUMER_GROUP in your .env file."
        )
        sys.exit(1)

    # Load seen keys directly from consumed_events.jsonl
    seen_keys = load_seen_keys_from_file(CONSUMED_EVENTS_FILE)

    logger.info(f"Starting Kafka producer -> topic '{KAFKA_TOPIC}'")
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    try:
        rows = fetch_rows()
    except Exception:
        logger.exception("Failed to fetch data from Databricks")
        sys.exit(1)

    sent = 0
    skipped_duplicates = 0

    for row in rows:
        event = build_event(row)

        # Check if key has already been processed in this batch
        if event["event_id"] in seen_keys:
            logger.warning(f"Skipping duplicate event_id key: '{event['event_id']}'")
            skipped_duplicates += 1
            continue

        seen_keys.add(event["event_id"])
        payload = json.dumps(event)

        try:
            producer.produce(
                topic=KAFKA_TOPIC,  # type: ignore
                key=event["event_id"],
                value=payload,
                callback=delivery_report,
            )
        except BufferError:
            logger.warning("Local producer queue full, flushing before retrying")
            producer.flush()
            producer.produce(
                topic=KAFKA_TOPIC,  # type: ignore
                key=event["event_id"],
                value=payload,
                callback=delivery_report,
            )

        producer.poll(0)  # trigger delivery callbacks without blocking
        sent += 1

    logger.info(
        f"Flushing producer ({sent} unique events queued, {skipped_duplicates} duplicates skipped)..."
    )
    producer.flush()
    logger.info(f"Producer finished. {sent} events sent to '{KAFKA_TOPIC}'.")


if __name__ == "__main__":
    main()
