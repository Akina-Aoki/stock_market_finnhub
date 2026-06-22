import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


# Load environment variables from .env
load_dotenv()

# Finnhub API configuration
API_KEY = os.getenv("FINNHUB_API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"

# Keep MVP small to avoid API/rate-limit problems
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

KAFKA_TOPIC = "stock_quotes"


if not API_KEY:
    raise ValueError("FINNHUB_API_KEY is missing. Add it to your .env file.")


producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


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


def main() -> None:
    """Continuously fetch stock quotes and publish them to Kafka."""
    print(f"Starting producer. Sending messages to Kafka topic: {KAFKA_TOPIC}")

    while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)

            if quote:
                print(f"Producing: {quote}")
                producer.send(KAFKA_TOPIC, value=quote)

        producer.flush()
        time.sleep(30)


if __name__ == "__main__":
    main()