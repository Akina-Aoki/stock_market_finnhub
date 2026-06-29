# Finnhub → Kafka → Amazon S3 Ingestion Documentation

## 1. Overview

This document explains the first ingestion part of the stock market data pipeline.

The goal of this phase is to collect stock quote data from the Finnhub API, send it into Kafka as streaming messages, consume those messages, and store them as raw JSONL files in Amazon S3.

This covers the first part of the architecture:

```text
Finnhub API
→ Python Producer
→ Kafka Topic: stock_quotes
→ Python Consumer
→ Amazon S3 Bucket: finnhub-stocks
```

The next phase will be:

```text
Amazon S3
→ Snowflake raw table
→ dbt transformations
→ dashboard / Streamlit app
```

---

## 2. Why this project is different from static CSV projects

In previous projects, the data was usually static.

Example:

```text
CSV file
→ uploaded to S3
→ loaded into Snowflake
→ transformed with SQL/dbt
```

In this project, the data is generated continuously from an API.

Example:

```text
Finnhub API gives latest stock quote
→ producer sends it to Kafka
→ Kafka stores it as a message
→ consumer reads the message
→ consumer saves it to S3
```

This means the pipeline is closer to a near-real-time data ingestion pipeline.

It is not fully live like a trading system, but it collects repeated stock price snapshots over time.

---

## 3. Tools used in this phase

| Tool           | Purpose                                                       |
| -------------- | ------------------------------------------------------------- |
| Finnhub API    | Source of stock quote data                                    |
| Python         | Used to build producer and consumer scripts                   |
| Kafka          | Streaming/message broker layer                                |
| Kafdrop        | Browser UI for checking Kafka topics and messages             |
| Amazon S3      | Raw storage layer                                             |
| boto3          | Python library for uploading files to AWS S3                  |
| Docker Compose | Runs Kafka, Zookeeper, Kafdrop, Airflow, and Postgres locally |

---

## 4. Current Docker services

The project currently runs these services with Docker Compose:

```text
zookeeper
kafka
kafdrop
airflow-postgres
airflow-scheduler
airflow-webserver
```

MinIO was removed because this project uses real Amazon S3 instead.

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

## 5. Kafka topic

The Kafka topic used in this project is:

```text
stock_quotes
```

The topic was created with:

```text
Partitions: 3
Replication factor: 1
```

The topic can be created from Kafdrop or from the terminal.

Terminal command:

```bash
docker exec kafka kafka-topics \
  --create \
  --topic stock_quotes \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

To check existing topics:

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

Expected output:

```text
stock_quotes
```

---

## 6. Kafka beginner explanation

Kafka is used as a temporary message storage layer.

In this project:

```text
Producer = sends stock quotes into Kafka
Topic = Kafka inbox where messages are stored
Consumer = reads messages from Kafka
Partition = lane inside a topic
Offset = message number inside a partition
```

The topic is:

```text
stock_quotes
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

---

## 7. Environment variables

Secrets and configuration are stored in a local `.env` file.

The `.env` file is not committed to GitHub.

Example `.env` structure:

```env
FINNHUB_API_KEY=your_finnhub_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=eu-north-1
S3_BUCKET_NAME=finnhub-stocks
```

Important:

```text
.env must stay in .gitignore
```

The project should never hardcode API keys or AWS credentials inside Python files.

---

## 8. Producer: Finnhub API → Kafka

The producer script is:

```text
producer/producer.py
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

The producer fetches data every 30 seconds.

### Producer flow

```text
1. Load Finnhub API key from .env
2. Connect to Kafka at localhost:29092
3. Fetch latest quote for AAPL, MSFT, TSLA, GOOGL, and AMZN
4. Convert each quote into a Python dictionary
5. Send each dictionary as a Kafka message
6. Repeat every 30 seconds
```

### Example producer output

```text
Starting producer. Sending messages to Kafka topic: stock_quotes
Connected producer to Kafka bootstrap server: localhost:29092

Producing: {'symbol': 'AAPL', 'current_price': 283.78, ...}
Sent message to Kafka | topic=stock_quotes, partition=0, offset=0, symbol=AAPL
```

This confirms that the producer successfully sent stock quote messages to Kafka.

---

## 9. Consumer: Kafka → Amazon S3

The consumer script is:

```text
consumer/consumer.py
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

### Consumer flow

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

### Why the consumer batches messages

The consumer does not upload one file per Kafka message.

Instead, it batches messages.

Current batch settings:

```python
BATCH_SIZE = 25
BATCH_TIMEOUT_SECONDS = 60
```

This means the consumer uploads when either:

```text
25 messages are collected
```

or

```text
enough time has passed and new messages arrive
```

This is better because it avoids creating too many tiny files in S3.

---

## 10. S3 raw file structure

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

## 11. JSONL raw data format

The consumer saves data as JSONL.

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

JSONL is useful for data pipelines because it is easier to process line by line.

---

## 12. Why the order in S3 is not sorted

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

---

## 13. How to run this phase

### Step 1: Start Docker services

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

Important containers:

```text
zookeeper
kafka
kafdrop
```

### Step 2: Create Kafka topic if missing

Check topic:

```bash
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server kafka:9092
```

If `stock_quotes` is missing, create it:

```bash
docker exec kafka kafka-topics \
  --create \
  --topic stock_quotes \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

### Step 3: Run producer

Terminal 1:

```bash
cd ~/de25/stock_market_finnhub
source .venv_stock/Scripts/activate
python producer/producer.py
```

This sends stock quote messages to Kafka.

### Step 4: Run consumer

Terminal 2:

```bash
cd ~/de25/stock_market_finnhub
source .venv_stock/Scripts/activate
python consumer/consumer.py
```

This reads Kafka messages and uploads them to S3.

---

## 14. Validation completed

The consumer successfully uploaded one raw batch file to S3.

Confirmed S3 path:

```text
s3://finnhub-stocks/raw/stock_quotes/ingestion_date=2026-06-29/hour=07/batch_20260629T075131Z.jsonl
```

The uploaded file size was around:

```text
6.5 KB
```

The file contained stock quote records for:

```text
AAPL
MSFT
TSLA
GOOGL
AMZN
```

This proves the ingestion flow is working.

---

## 15. Current project status

Completed:

```text
✅ Docker infrastructure is running
✅ Kafka is running
✅ Kafdrop is working
✅ Kafka topic stock_quotes was created
✅ producer.py fetches data from Finnhub
✅ producer.py sends messages to Kafka
✅ consumer.py reads messages from Kafka
✅ consumer.py uploads raw JSONL batches to Amazon S3
✅ S3 bucket finnhub-stocks contains raw stock quote files
```

Current completed pipeline:

```text
Finnhub API
→ Python producer
→ Kafka topic: stock_quotes
→ Python consumer
→ Amazon S3 raw JSONL storage
```

---

## 16. Current limitations

This phase is still ingestion only.

Current limitations:

```text
- Data is stored as raw JSONL only
- No Snowflake loading yet
- No dbt transformations yet
- No dashboard yet
- Kafka topic disappears after docker compose down because Kafka storage is not persistent yet
- Producer fetches every 30 seconds, so it should not be left running for many hours during development
```

---

## 17. Future improvements

Possible improvements:

```text
- Add persistent Kafka volume so topics do not disappear after restart
- Use stock symbol as Kafka message key to keep each symbol in the same partition
- Add stronger AWS least-privilege IAM policy instead of broad S3 access
- Add local logging
- Add Airflow DAG to orchestrate producer/consumer or batch ingestion
- Load S3 JSONL files into Snowflake
- Transform raw data with dbt into staging, intermediate, and mart models
- Build Snowflake dashboard or Streamlit app
```

---

## 18. Next phase

The next phase is:

```text
Amazon S3 raw JSONL files
→ Snowflake raw landing table
```

The expected Snowflake flow will be:

```text
1. Create Snowflake database, schema, and warehouse
2. Create raw table for stock quote JSON data
3. Create file format for JSONL
4. Create external stage pointing to S3
5. Load raw JSONL files from S3 into Snowflake
6. Validate row count
```

After that, dbt will transform the raw Snowflake table into clean analytics models.

Expected future pipeline:

```text
Finnhub API
→ Kafka
→ S3 raw JSONL
→ Snowflake raw table
→ dbt staging
→ dbt intermediate
→ dbt marts
→ Dashboard
```
