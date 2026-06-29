"""
consumer.py

This script reads stock quote messages from the Kafka topic `stock_quotes`
and saves them as raw JSONL files in an Amazon S3 bucket.

Flow:
1. Connect to Kafka.
2. Read stock quote messages from the stock_quotes topic.
3. Collect messages into a small batch.
4. Upload the batch to Amazon S3.
5. Commit Kafka offsets only after the S3 upload succeeds.
"""

import json
import os # read environment variables from .env
import time
from datetime import datetime, timezone # create timestamps for S3 folder/file names
from typing import Any

import boto3 # Python library for talking to AWS
from botocore.exceptions import BotoCoreError, ClientError #  AWS-related error handling
from dotenv import load_dotenv
from kafka import KafkaConsumer # reads messages from Kafka
from kafka.errors import KafkaError


# Load local environment variables from .env
load_dotenv()


# Kafka configuration.
# Use localhost:29092 when running this script from your Windows/Git Bash terminal.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

KAFKA_TOPIC = "stock_quotes"
KAFKA_GROUP_ID = "stock-quotes-s3-consumer"


# AWS S3 configuration.
# finnhub-stcoks bucket already exists in AWS.
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "finnhub-stocks")

# Optional, but useful for organizing files inside the bucket.
S3_RAW_PREFIX = "raw/stock_quotes"


# Batch configuration.
# We do NOT want to upload one tiny S3 file per Kafka message.
# This keeps S3 cleaner and reduces PUT requests.
BATCH_SIZE = 25
BATCH_TIMEOUT_SECONDS = 60


def create_s3_client():
    """
    Create an AWS S3 client.

    boto3 will use AWS credentials from .env variables, for example:
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION

    These should be stored in .env locally and never committed to GitHub.
    """
    return boto3.client("s3")


def check_bucket_exists(s3_client) -> None:
    """
    Check that the target S3 bucket exists and is accessible.

    Do not create the bucket here because you already created it manually
    in the AWS console.
    """
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        print(f"S3 bucket is accessible: {S3_BUCKET_NAME}")

    except ClientError as error:
        raise RuntimeError(
            f"Cannot access S3 bucket '{S3_BUCKET_NAME}'. "
            "Check the bucket name, AWS credentials, and region."
        ) from error


def create_consumer() -> KafkaConsumer:
    """
    Create a Kafka consumer.

    A consumer reads messages from a Kafka topic.
    Here, it reads stock quote messages from the stock_quotes topic.
    """
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],

            # Start from the beginning if this consumer group has no saved offset yet.
            auto_offset_reset="earliest",

            # We manually commit offsets only after successful S3 upload.
            enable_auto_commit=False,

            # Consumer group name.
            group_id=KAFKA_GROUP_ID,

            # Convert Kafka message bytes back into a Python dictionary.
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

        print(f"Connected consumer to Kafka bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
        return consumer

    except KafkaError as error:
        raise RuntimeError(f"Failed to create Kafka consumer: {error}") from error


def build_s3_key() -> str:
    """
    Build a partitioned S3 object key for the current batch.

    Example:
    raw/stock_quotes/ingestion_date=2026-06-29/hour=06/batch_20260629T061500Z.jsonl
    """
    now = datetime.now(timezone.utc)

    ingestion_date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    batch_timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    return (
        f"{S3_RAW_PREFIX}/"
        f"ingestion_date={ingestion_date}/"
        f"hour={hour}/"
        f"batch_{batch_timestamp}.jsonl"
    )


def upload_batch_to_s3(s3_client, records: list[dict[str, Any]]) -> None:
    """
    Upload a batch of Kafka records to Amazon S3 as a JSONL file.

    JSONL means JSON Lines:
    - one JSON object per line
    - easier for data pipelines than one giant JSON array
    """
    if not records:
        return

    s3_key = build_s3_key()

    # Convert list of dictionaries into JSON Lines format.
    body = "\n".join(json.dumps(record) for record in records)

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )

        print(
            f"Uploaded {len(records)} records to "
            f"s3://{S3_BUCKET_NAME}/{s3_key}"
        )

    except (BotoCoreError, ClientError) as error:
        raise RuntimeError(f"Failed to upload batch to S3: {error}") from error


def main() -> None:
    """
    Main consumer workflow.

    This function:
    1. Connects to S3.
    2. Connects to Kafka.
    3. Reads messages from Kafka.
    4. Batches messages.
    5. Uploads each batch to S3.
    """
    print("Starting Kafka consumer for stock quotes...")
    print(f"Target S3 bucket: {S3_BUCKET_NAME}")

    s3_client = create_s3_client()
    check_bucket_exists(s3_client)

    consumer = create_consumer()

    batch = []
    last_upload_time = time.time()

    try:
        for message in consumer:
            record = message.value
            batch.append(record)

            symbol = record.get("symbol", "unknown")
            print(
                f"Consumed message | "
                f"topic={message.topic}, "
                f"partition={message.partition}, "
                f"offset={message.offset}, "
                f"symbol={symbol}"
            )

            batch_is_full = len(batch) >= BATCH_SIZE
            batch_is_old = (time.time() - last_upload_time) >= BATCH_TIMEOUT_SECONDS

            if batch_is_full or batch_is_old:
                upload_batch_to_s3(s3_client, batch)

                # Commit Kafka offsets only after S3 upload succeeds.
                consumer.commit()

                batch = []
                last_upload_time = time.time()

    except KeyboardInterrupt:
        print("Consumer stopped manually.")

        # Upload remaining records before exiting.
        if batch:
            upload_batch_to_s3(s3_client, batch)
            consumer.commit()

    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    main()