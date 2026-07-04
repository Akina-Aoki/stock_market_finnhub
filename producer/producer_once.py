"""
producer_once.py

Airflow-friendly one-shot producer for Finnhub stock quotes.

This script fetches exactly one quote for each configured stock symbol and
publishes one Kafka message per symbol to the stock_quotes topic. It exits after
all messages are flushed and the Kafka producer is closed.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


load_dotenv()

API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_CONNECT_RETRIES = int(os.getenv("KAFKA_CONNECT_RETRIES", "6"))
KAFKA_CONNECT_RETRY_DELAY_SECONDS = int(
    os.getenv("KAFKA_CONNECT_RETRY_DELAY_SECONDS", "10")
)
KAFKA_TOPIC = "stock_quotes"
RUN_ID = os.getenv("RUN_ID")


def build_run_id() -> str:
    """Return the provided RUN_ID or generate one for this producer run."""
    if RUN_ID:
        return RUN_ID

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short_uuid = uuid.uuid4().hex[:8]
    return f"{timestamp}_{short_uuid}"


if not API_KEY:
    raise ValueError("FINNHUB_API_KEY is missing. Add it to your environment or .env file.")


def create_producer() -> KafkaProducer:
    """Create a Kafka producer that serializes messages as JSON bytes."""
    last_error: KafkaError | None = None

    for attempt in range(1, KAFKA_CONNECT_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=3,
                request_timeout_ms=30000,
                max_block_ms=30000,
            )
            print(
                "Connected Kafka producer to bootstrap server: "
                f"{KAFKA_BOOTSTRAP_SERVERS}"
            )
            return producer
        except KafkaError as error:
            last_error = error
            print(
                "Kafka producer bootstrap failed "
                f"(attempt {attempt}/{KAFKA_CONNECT_RETRIES}): {error}."
            )

            if attempt == KAFKA_CONNECT_RETRIES:
                break

            print(f"Retrying in {KAFKA_CONNECT_RETRY_DELAY_SECONDS} seconds...")
            time.sleep(KAFKA_CONNECT_RETRY_DELAY_SECONDS)

    raise RuntimeError(f"Failed to create Kafka producer: {last_error}") from last_error


def fetch_quote(symbol: str, run_id: str) -> dict:
    """Fetch one quote from Finnhub and return a Kafka-ready message."""
    params = {
        "symbol": symbol,
        "token": API_KEY,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        raise RuntimeError(f"Failed to fetch quote for {symbol}: {error}") from error

    payload = response.json()
    fetched_at = datetime.now(timezone.utc).isoformat()

    return {
        "run_id": run_id,
        "symbol": symbol,
        "fetched_at": fetched_at,
        "current_price": payload.get("c"),
        "change": payload.get("d"),
        "percent_change": payload.get("dp"),
        "high_price": payload.get("h"),
        "low_price": payload.get("l"),
        "open_price": payload.get("o"),
        "previous_close_price": payload.get("pc"),
        "finnhub_timestamp": payload.get("t"),
    }


def send_quote(producer: KafkaProducer, quote: dict) -> None:
    """Send one quote to Kafka and wait for broker acknowledgement."""
    try:
        future = producer.send(KAFKA_TOPIC, value=quote)
        metadata = future.get(timeout=30)
        print(
            "Sent quote to Kafka | "
            f"topic={metadata.topic}, partition={metadata.partition}, "
            f"offset={metadata.offset}, symbol={quote['symbol']}"
        )
    except KafkaError as error:
        raise RuntimeError(f"Failed to send quote for {quote.get('symbol')}: {error}") from error


def main() -> None:
    """Fetch and publish one quote for each symbol, then exit."""
    run_id = build_run_id()
    print(f"Starting one-shot Finnhub producer for topic: {KAFKA_TOPIC}")
    print(f"Producer run_id: {run_id}")
    print(f"Symbols: {', '.join(SYMBOLS)}")

    producer = create_producer()
    sent_count = 0

    try:
        for symbol in SYMBOLS:
            print(f"Fetching quote for {symbol}...")
            quote = fetch_quote(symbol, run_id)
            send_quote(producer, quote)
            sent_count += 1

        producer.flush()
        print(f"Producer completed successfully for run_id={run_id}. Sent {sent_count} messages.")
    finally:
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    main()