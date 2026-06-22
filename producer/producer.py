import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


# Load environment variables from .env
load_dotenv()


# Finnhub API configuration
API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"

# Keep MVP small to avoid API/rate-limit problems
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]


# Kafka configuration
# Use localhost:29092 when running producer.py from your Windows/Git Bash terminal.
# Use kafka:9092 only when running Python inside another Docker container.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

KAFKA_TOPIC = "stock_quotes"


if not API_KEY:
    raise ValueError("FINNHUB_API_KEY is missing. Add it to your .env file.")


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            retries=3,
            request_timeout_ms=30000,
            max_block_ms=30000,
        )

        print(f"Connected producer to Kafka bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer

    except KafkaError as error:
        raise RuntimeError(f"Failed to create Kafka producer: {error}") from error


def fetch_quote(symbol: str) -> dict | None:
    """Fetch the latest stock quote for one symbol from Finnhub."""
    params = {
        "symbol": symbol,
        "token": API_KEY,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        quote = {
            "symbol": symbol,
            "current_price": data.get("c"),
            "change": data.get("d"),
            "percent_change": data.get("dp"),
            "high_price": data.get("h"),
            "low_price": data.get("l"),
            "open_price": data.get("o"),
            "previous_close_price": data.get("pc"),
            "finnhub_timestamp": data.get("t"),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        return quote

    except requests.RequestException as error:
        print(f"Error fetching {symbol}: {error}")
        return None


def send_quote(producer: KafkaProducer, quote: dict) -> None:
    """Send one quote message to Kafka."""
    try:
        future = producer.send(KAFKA_TOPIC, value=quote)

        # Wait for Kafka acknowledgement.
        record_metadata = future.get(timeout=30)

        print(
            "Sent message to Kafka | "
            f"topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}, "
            f"symbol={quote['symbol']}"
        )

    except KafkaTimeoutError as error:
        print(
            "Kafka timeout error. The producer could not fetch Kafka metadata. "
            "Check that Docker is running and that KAFKA_BOOTSTRAP_SERVERS=localhost:29092."
        )
        raise error

    except KafkaError as error:
        print(f"Kafka error while sending message: {error}")
        raise error


def main() -> None:
    """Continuously fetch stock quotes and publish them to Kafka."""
    print(f"Starting producer. Sending messages to Kafka topic: {KAFKA_TOPIC}")

    producer = create_producer()

    try:
        while True:
            for symbol in SYMBOLS:
                quote = fetch_quote(symbol)

                if quote:
                    print(f"Producing: {quote}")
                    send_quote(producer, quote)

            producer.flush()
            print("Batch completed. Sleeping for 30 seconds...\n")
            time.sleep(30)

    except KeyboardInterrupt:
        print("Producer stopped manually.")

    finally:
        producer.flush()
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    main()