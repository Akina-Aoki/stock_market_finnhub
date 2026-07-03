"""
consumer_once.py

Airflow-friendly one-shot Kafka consumer for Finnhub stock quotes.

This script consumes a finite batch of messages from the stock_quotes topic,
writes the batch to Amazon S3 as JSONL, commits Kafka offsets only after the S3
upload succeeds, and then exits.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = "stock_quotes"
RUN_ID = os.getenv("RUN_ID")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID") or (
    f"stock-quotes-s3-consumer-once-{RUN_ID}" if RUN_ID else "stock-quotes-s3-consumer-once"
)
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "finnhub-stocks")
S3_RAW_PREFIX = "raw/stock_quotes"
MESSAGE_COUNT = int(os.getenv("MESSAGE_COUNT", "5"))
CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "30000"))


if not RUN_ID:
    raise ValueError(
        "RUN_ID is missing. Provide the same RUN_ID used by producer_once.py "
        "so this consumer only writes records from that producer run."
    )

if MESSAGE_COUNT <= 0:
    raise ValueError("MESSAGE_COUNT must be greater than zero.")


def create_consumer() -> KafkaConsumer:
    """Create a Kafka consumer that reads JSON messages from stock_quotes."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=KAFKA_GROUP_ID,
            consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        print(f"Connected Kafka consumer to bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Reading up to {MESSAGE_COUNT} messages from topic: {KAFKA_TOPIC}")
        print(f"Kafka consumer group: {KAFKA_GROUP_ID}")
        return consumer
    except KafkaError as error:
        raise RuntimeError(f"Failed to create Kafka consumer: {error}") from error


def build_s3_key(run_id: str) -> str:
    """Build a partitioned S3 key for the current one-shot batch."""
    now = datetime.now(timezone.utc)
    ingestion_date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    batch_timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    unique_id = uuid.uuid4().hex

    return (
        f"{S3_RAW_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"hour={hour}/"
        f"batch_{run_id}_{batch_timestamp}_{unique_id}.jsonl"
    )


def upload_batch_to_s3(records: list[dict[str, Any]], run_id: str) -> str:
    """Upload records to S3 as JSONL and return the written S3 key."""
    if not records:
        raise ValueError("No messages were consumed; refusing to upload an empty batch.")

    s3_key = build_s3_key(run_id)
    body = "\n".join(json.dumps(record) for record in records)
    s3_client = boto3.client("s3")

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as error:
        raise RuntimeError(f"Failed to upload batch to S3: {error}") from error

    print(f"Uploaded {len(records)} records to s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key


def consume_batch(consumer: KafkaConsumer, run_id: str) -> tuple[list[dict[str, Any]], int]:
    """Consume messages until MESSAGE_COUNT matching records are collected."""
    records: list[dict[str, Any]] = []
    ignored_count = 0

    for message in consumer:
        message_run_id = message.value.get("run_id")
        symbol = message.value.get("symbol", "unknown")

        if message_run_id == run_id:
            records.append(message.value)
            print(
                "Consumed matching message | "
                f"topic={message.topic}, partition={message.partition}, "
                f"offset={message.offset}, symbol={symbol}, run_id={message_run_id}"
            )
        else:
            ignored_count += 1
            print(
                "Ignored message with non-matching run_id | "
                f"topic={message.topic}, partition={message.partition}, "
                f"offset={message.offset}, symbol={symbol}, run_id={message_run_id}"
            )

        if len(records) >= MESSAGE_COUNT:
            break

    return records, ignored_count


def main() -> None:
    """Consume a finite batch, write it to S3, commit offsets, and exit."""
    print("Starting one-shot Kafka-to-S3 consumer.")
    print(f"Target S3 bucket: {S3_BUCKET_NAME}")
    print(f"Filtering for run_id: {RUN_ID}")

    consumer = create_consumer()

    try:
        records, ignored_count = consume_batch(consumer, RUN_ID)
        if not records:
            raise RuntimeError(
                f"No messages matching RUN_ID={RUN_ID} consumed from topic '{KAFKA_TOPIC}' "
                f"within {CONSUMER_TIMEOUT_MS} ms. Ignored {ignored_count} messages."
            )

        s3_key = upload_batch_to_s3(records, RUN_ID)
        consumer.commit()
        print("Kafka offsets committed after successful S3 upload.")
        print(f"Matching messages consumed: {len(records)}")
        print(f"Ignored messages consumed: {ignored_count}")
        print(f"Consumer completed successfully. S3 key written: {s3_key}")
    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    main()