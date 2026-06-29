
"""
producer.py

This script fetches stock quote data from the Finnhub API
and sends each stock quote as a message to a Kafka topic.

Flow:
1. Read secrets/settings from the .env file.
2. Fetch stock prices from Finnhub.
3. Create a Kafka producer.
4. Send each stock quote to the Kafka topic called stock_quotes.
5. Repeat every 30 seconds.
"""

import json
import os #  read environment variables from .env
import time
from datetime import datetime, timezone # create timestamps for S3 folder/file names

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer # reads messages for Kafka
from kafka.errors import KafkaError, KafkaTimeoutError


# Load environment variables from the .env file.
# This lets us keep secrets like the Finnhub API key outside the Python code.
load_dotenv()


# Finnhub API configuration.
# The API key comes from the .env file.
API_KEY = os.getenv("FINNHUB_API_KEY")

# Finnhub endpoint used to get the latest quote for one stock symbol.
BASE_URL = "https://finnhub.io/api/v1/quote"

# List of stock symbols we want to fetch.
# Keeping this small helps avoid API rate-limit problems.
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]


# Kafka configuration.
# This is the address the producer uses to connect to Kafka.
#
# Use localhost:29092 when running producer.py from Windows/Git Bash terminal.
# Use kafka:9092 only when running Python inside another Docker container.
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

# Kafka topic where all stock quote messages will be sent.
KAFKA_TOPIC = "stock_quotes"


# Stop the script early if the Finnhub API key is missing.
# This prevents confusing API errors later.
if not API_KEY:
    raise ValueError("FINNHUB_API_KEY is missing. Add it to your .env file.")


def create_producer() -> KafkaProducer:
    """
    Create and return a Kafka producer.

    A Kafka producer is responsible for sending messages to Kafka.
    Each message is one stock quote.

    The value_serializer converts Python dictionaries into JSON bytes,
    because Kafka messages must be sent as bytes.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],

            # Convert each Python dictionary into JSON, then encode it as UTF-8 bytes.
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),

            # Retry a few times if sending a message fails temporarily.
            retries=3,

            # How long the producer waits for Kafka requests before timing out.
            request_timeout_ms=30000,

            # How long the producer waits when Kafka metadata is not available.
            max_block_ms=30000,
        )

        print(f"Connected producer to Kafka bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer

    except KafkaError as error:
        # If the producer cannot connect to Kafka, raise a clear error.
        raise RuntimeError(f"Failed to create Kafka producer: {error}") from error


def fetch_quote(symbol: str) -> dict | None:
    """
    Fetch the latest stock quote for one stock symbol from Finnhub.

    Example:
    symbol = "AAPL"

    Returns:
        A dictionary containing cleaned quote data if the API call works.
        None if the API request fails.
    """

    # Query parameters sent to Finnhub.
    # Example URL will become:
    # https://finnhub.io/api/v1/quote?symbol=AAPL&token=YOUR_KEY
    params = {
        "symbol": symbol,
        "token": API_KEY,
    }

    try:
        # Send GET request to Finnhub.
        response = requests.get(BASE_URL, params=params, timeout=10)

        # Raise an error if Finnhub returns a bad status code.
        response.raise_for_status()

        # Convert the API response from JSON text into a Python dictionary.
        data = response.json()

        # Create our own clearer dictionary.
        # Finnhub uses short column names like c, d, dp, h, l, o, pc, and t.
        # Here we rename them into more understandable names.
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

            # Add timestamp for when fetched the data.
            # This is useful later for tracking ingestion time.
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

        return quote

    except requests.RequestException as error:
        # If the API request fails, print the error and continue.
        print(f"Error fetching {symbol}: {error}")
        return None


def send_quote(producer: KafkaProducer, quote: dict) -> None:
    """
    Send one stock quote message to Kafka.

    Args:
        producer: The Kafka producer created by create_producer().
        quote: One stock quote dictionary from fetch_quote().
    """
    try:
        # Send the quote dictionary to the Kafka topic.
        # Kafka will store this as one message/event.
        future = producer.send(KAFKA_TOPIC, value=quote)

        # Wait for Kafka to confirm that the message was received.
        # This gives us metadata like topic, partition, and offset.
        record_metadata = future.get(timeout=30)

        # Print confirmation so we can see where Kafka stored the message.
        print(
            "Sent message to Kafka | "
            f"topic={record_metadata.topic}, "
            f"partition={record_metadata.partition}, "
            f"offset={record_metadata.offset}, "
            f"symbol={quote['symbol']}"
        )

    except KafkaTimeoutError as error:
        # This usually means Python cannot reach Kafka.
        # Common cause: wrong bootstrap server or Docker not running.
        print(
            "Kafka timeout error. The producer could not fetch Kafka metadata. "
            "Check that Docker is running and that KAFKA_BOOTSTRAP_SERVERS=localhost:29092."
        )
        raise error

    except KafkaError as error:
        # Handle other Kafka-related errors.
        print(f"Kafka error while sending message: {error}")
        raise error


def main() -> None:
    """
    Main workflow for the producer.

    This function:
    1. Creates the Kafka producer.
    2. Loops forever.
    3. Fetches stock quotes for each symbol.
    4. Sends each quote to Kafka.
    5. Waits 30 seconds before the next batch.
    """
    print(f"Starting producer. Sending messages to Kafka topic: {KAFKA_TOPIC}")

    # Create one Kafka producer and reuse it for all messages.
    producer = create_producer()

    try:
        # Infinite loop.
        # This keeps the producer running until you stop it with Ctrl + C.
        while True:

            # Fetch and send one quote for each stock symbol.
            for symbol in SYMBOLS:
                quote = fetch_quote(symbol)

                # Only send the quote if the API request worked.
                if quote:
                    print(f"Producing: {quote}")
                    send_quote(producer, quote)

            # Make sure all pending messages are sent before sleeping.
            producer.flush()

            # Wait before fetching the next batch.
            print("Batch completed. Sleeping for 30 seconds...\n")
            time.sleep(30)

    except KeyboardInterrupt:
        # This runs when you manually stop the script with Ctrl + C.
        print("Producer stopped manually.")

    finally:
        # Always flush and close the producer before exiting.
        # This helps avoid losing unsent messages.
        producer.flush()
        producer.close()
        print("Kafka producer closed.")


# This makes sure main() only runs when this file is executed directly.
# It will not auto-run if this file is imported into another Python file.
if __name__ == "__main__":
    main()

