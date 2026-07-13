# Finnhub → Kafka → Amazon S3 Ingestion Documentation

## ✅ Overview

This phase is to collect stock quote data from the Finnhub API, send it into Kafka as streaming messages, consume those messages, and store them as raw JSONL files in Amazon S3.


### ✅ Producer flow

```text
1. Load Finnhub API key from .env
2. Connect to Kafka at localhost:29092
3. Fetch latest quote for AAPL, MSFT, TSLA, GOOGL, and AMZN
4. Convert each quote into a Python dictionary
5. Send each dictionary as a Kafka message
6. Repeat every 1 hour
```

### ✅ Consumer flow

```text
1. Load AWS credentials and Kafka settings from .env
2. Connect to Amazon S3 using boto3
3. Check that the finnhub-stocks bucket is accessible
4. Connect to Kafka
5. Read messages from the stock_quotes topic
6. Collect messages into a batch
7. Upload the batch to S3 as a JSONL file
8. Commit Kafka offsets after successful S3 upload
```


## ✅ Current Docker services

these services runs with Docker Compose:


Kafdrop is available in the browser here:

```text
http://localhost:9000
```


## ✅ Kafka topic (Show Kafkadrop UI)

The Kafka topic used in this project is:

```text
stock_quotes
```

The topic was created with:

```text
Partitions: 3
```

Because it has 3 partitions, this means Kafka stored each stock quote message inside a partition.

Kafka chooses the partition automatically. 
Later, a possible improvement is to use the stock symbol as the message key so all messages for the same company go to the same partition.



## ✅ S3 raw file structure (Show S3)

I use S3 as the raw storage layer to keep the original JSONL files before transforming them. 

If something goes wrong later, I can always go back to the original raw files.
The S3 bucket is:

```text
finnhub-stocks
```

The raw files are stored under:

```text
raw/stock_quotes/
```

This structure organizes raw data by ingestion date and hour.

---

## ✅ JSONL raw data format (Show JSONL sample)

The consumer saves data as JSONL. JSONL is useful for data pipelines because it is easier to process line by line.


JSONL means:

```text
One JSON object per line
```

Example:

```json
{"symbol": "MSFT", "current_price": 372.97, "change": 20.14, "percent_change": 5.7081, "high_price": 376.61, "low_price": 355.43, "open_price": 357.15, "previous_close_price": 352.83, "finnhub_timestamp": 1782504000, "fetched_at": "2026-06-29T06:10:05.772942+00:00"}
{"symbol": "GOOGL", "current_price": 337.39, "change": -6.32, "percent_change": -1.8388, "high_price": 346.36, "low_price": 330.2, "open_price": 342.55, "previous_close_price": 343.71, "finnhub_timestamp": 1782504000, "fetched_at": "2026-06-29T06:10:06.707984+00:00"}
{"symbol": "AMZN", "current_price": 232.69, "change": 5.68, "percent_change": 2.5021, "high_price": 233.9, "low_price": 226.125, "open_price": 227.205, "previous_close_price": 227.01, "finnhub_timestamp": 1782504000, "fetched_at": "2026-06-29T06:10:07.195909+00:00"}
```
