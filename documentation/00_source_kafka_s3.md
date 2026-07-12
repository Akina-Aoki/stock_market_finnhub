# Finnhub → Kafka → Amazon S3 Ingestion Documentation

## ✅ Overview

The goal of this phase is to collect stock quote data from the Finnhub API, send it into Kafka as streaming messages, consume those messages, and store them as raw JSONL files in Amazon S3.

This covers the first part of the architecture:

```text
Finnhub API
→ Python Producer
→ Kafka Topic: stock_quotes
→ Python Consumer
→ Amazon S3 Bucket: finnhub-stocks
```


## ✅ In this project, the data is generated continuously from an API.


```text
Finnhub API gives latest stock quote
→ producer sends it to Kafka
→ Kafka stores it as a message
→ consumer reads the message
→ consumer saves it to S3
```


## ✅ Tools used in this phase

| Tool           | Purpose                                                       |
| -------------- | ------------------------------------------------------------- |
| Finnhub API    | Source of stock quote data                                    |
| Python         | Used to build producer and consumer scripts                   |
| Kafka          | Streaming/message broker layer                                |
| Kafdrop        | Browser UI for checking Kafka topics and messages             |
| Amazon S3      | Raw storage layer                                             |
| boto3          | Python library for uploading files to AWS S3                  |
| Docker Compose | Runs Kafka, Zookeeper, Kafdrop, Airflow, and Postgres locally |

## ✅ Current Docker services

The project currently runs these services with Docker Compose:


Kafdrop is available in the browser here:

```text
http://localhost:9000
```

Airflow is available here:

```text
http://localhost:8080
```

At this stage, Airflow exists in the infrastructure but is not yet orchestrating the full pipeline.

---
| Service             | Easy description                                                                        |
| ------------------- | --------------------------------------------------------------------------------------- |
| `zookeeper`         | Helps Kafka manage and coordinate its broker.                                           |
| `kafka`             | Message broker that temporarily carries stock quote messages from producer to consumer. |
| `kafdrop`           | Web UI for checking Kafka topics and messages.                                          |
| `airflow-postgres`  | Database where Airflow stores DAG runs, task status, and metadata.                      |
| `airflow-scheduler` | Airflow service that decides when DAG tasks should run.                                 |
| `airflow-webserver` | Airflow browser UI where you trigger and monitor DAG runs.                              |



## ✅ Kafka topic
(Show Kafkadrop UI)

The Kafka topic used in this project is:

```text
stock_quotes
```

The topic was created with:

```text
Partitions: 3
Replication factor: 1
```

Because it has 3 partitions, it looks like this:

```text
stock_quotes
├── partition 0
├── partition 1
└── partition 2
```

Example Kafka output:

```text
AAPL  → partition 0, offset 0
MSFT  → partition 1, offset 0
TSLA  → partition 0, offset 1
GOOGL → partition 1, offset 1
AMZN  → partition 1, offset 2
```

This means Kafka stored each stock quote message inside a partition and assigned it an offset.

Offsets are counted separately inside each partition.

For this MVP, it is okay that Kafka chooses the partition automatically. Later, a possible improvement is to use the stock symbol as the Kafka message key so all messages for the same company go to the same partition.

## ✅ Producer: Finnhub API → Kafka

The producer script is:

```text
producer/producer_once.py
```

Its responsibility is to fetch stock quote data from Finnhub and send it to Kafka.

Current stock symbols:

```python
SYMBOLS = ["AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"]
```

The producer sends messages to:

```text
Kafka topic: stock_quotes
```

The producer fetches data every 1 hour.

### ✅ Producer flow

```text
1. Load Finnhub API key from .env
2. Connect to Kafka at localhost:29092
3. Fetch latest quote for AAPL, MSFT, TSLA, GOOGL, and AMZN
4. Convert each quote into a Python dictionary
5. Send each dictionary as a Kafka message
6. Repeat every 1 hour
```

## ✅ Consumer: Kafka → Amazon S3

The consumer script is:

```text
consumer/consumer_once.py
```

Its responsibility is to read messages from Kafka and save them into Amazon S3 as raw JSONL files.

The consumer reads from:

```text
Kafka topic: stock_quotes
```

The consumer uploads to:

```text
S3 bucket: finnhub-stocks
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



## ✅ S3 raw file structure
(Show S3)
The S3 bucket is:

```text
finnhub-stocks
```

The raw files are stored under:

```text
raw/stock_quotes/
```

Example S3 path:

```text
s3://finnhub-stocks/raw/stock_quotes/ingestion_date=2026-06-29/hour=07/batch_20260629T075131Z.jsonl
```

Folder structure:

```text
finnhub-stocks/
└── raw/
    └── stock_quotes/
        └── ingestion_date=2026-06-29/
            └── hour=07/
                └── batch_20260629T075131Z.jsonl
```

This structure is useful because it organizes raw data by ingestion date and hour.

---

## JSONL raw data format

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

This is different from a normal JSON array.

Normal JSON array:

```json
[
  {"symbol": "AAPL"},
  {"symbol": "MSFT"}
]
```

JSONL:

```json
{"symbol": "AAPL"}
{"symbol": "MSFT"}
```


## Why the order in S3 is not sorted

The data in the JSONL file may not appear in exact time order.

This is expected.

Kafka stores messages across multiple partitions, and the consumer reads from those partitions.

Kafka guarantees order inside one partition, but not globally across all partitions.

For analytics, this is not a problem because Snowflake and dbt can sort by:

```text
fetched_at
symbol
```

The raw layer keeps the data as it arrived. Sorting and cleaning happen later.

