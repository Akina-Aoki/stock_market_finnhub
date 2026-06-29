# Project Shutdown and Restart Guide

This guide explains how to safely stop and restart the Finnhub modern data stack project.

Current local services:

```text
Kafka
Zookeeper
Kafdrop
Airflow webserver
Airflow scheduler
Postgres
```

---

## 1. Before shutting down the PC

### Step 1: Stop running Python scripts

Check your terminals.

If any of these are still running:

```text
producer.py
consumer.py
test.py
```

stop them with:

```text
Ctrl + C
```

---

### Step 2: Stop Docker services

From the project root:

```bash
cd ~/de25/stock_market_finnhub
docker compose stop
```

This safely stops the Docker containers without deleting them.

Use `stop`, not `down`.

```bash
# Good
docker compose stop

# Avoid for now
docker compose down
```

`docker compose down` can remove containers and may cause the Kafka topic to disappear.

---

### Step 3: Close browsers and apps

After `docker compose stop`, it is safe to close:

```text
Airflow: http://localhost:8080
Kafdrop: http://localhost:9000
Snowflake
AWS Console
VS Code
Docker Desktop
```

You can now shut down your PC.

---

## 2. When starting work again

### Step 1: Open Docker Desktop

Start Docker Desktop first.

Wait until Docker is fully running.

---

### Step 2: Open Git Bash or VS Code terminal

Go to the project folder:

```bash
cd ~/de25/stock_market_finnhub
```

Activate the project virtual environment:

```bash
source .venv_stock/Scripts/activate
```

---

### Step 3: Start Docker services again

Run:

```bash
docker compose start
```

Wait around 30–60 seconds.

---

### Step 4: Check running containers

Run:

```bash
docker ps
```

You should see containers like:

```text
kafka
zookeeper
kafdrop
airflow-webserver
airflow-scheduler
airflow-postgres
```

---

## 3. Reopen local tools

Open Airflow:

```text
http://localhost:8080
```

Open Kafdrop:

```text
http://localhost:9000
```

Airflow may take 30–60 seconds to show DAGs.

Expected DAGs:

```text
dag_smoke_test
test_snowflake_connection
```

---

## 4. Check Kafka topic

Because Kafka does not currently have persistent storage configured, the topic may disappear after restart.

Check existing topics:

```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

Expected topic:

```text
stock_quotes
```

If `stock_quotes` is missing, recreate it:

```bash
docker exec kafka kafka-topics \
  --create \
  --topic stock_quotes \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```

---

## 5. Optional checks

### Check Airflow can see Snowflake environment variables

```bash
docker exec airflow-scheduler python -c "import os; print('ACCOUNT:', os.getenv('SNOWFLAKE_ACCOUNT')); print('USER:', os.getenv('SNOWFLAKE_USER')); print('DATABASE:', os.getenv('SNOWFLAKE_DATABASE'))"
```

Expected result:

```text
ACCOUNT: SGAGNXX-JO56492
USER: AIRAF
DATABASE: finnhub_stocks_mds
```

---

### Check Snowflake connection from Airflow

Use the Airflow UI and trigger this DAG:

```text
test_snowflake_connection
```

Expected result:

```text
success
```

The task log should show something like:

```text
Snowflake connection result:
('AIRAF', 'ACCOUNTADMIN', 'FINNHUB_STOCKS_MDS_WH', 'FINNHUB_STOCKS_MDS', 'RAW')

Airflow Snowflake connection successful.
```

---

## 6. What is safe after shutting down?

These are safe and will not disappear:

```text
Code files
SQL scripts
Markdown documentation
.env file locally
Snowflake database, schemas, tables, stage, and storage integration
AWS S3 bucket and JSONL files
Airflow DAG files inside dags/
```

These may need checking after restart:

```text
Kafka topic stock_quotes
Running Docker containers
Airflow DAG refresh time
```

---

## 7. Quick command summary

### Stop work

```bash
cd ~/de25/stock_market_finnhub
docker compose stop
```

Then close browsers, VS Code, Docker Desktop, and shut down the PC.

---

### Start work again

```bash
cd ~/de25/stock_market_finnhub
source .venv_stock/Scripts/activate
docker compose start
docker ps
```

Then open:

```text
http://localhost:8080
http://localhost:9000
```

Check Kafka topic:

```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

If missing:

```bash
docker exec kafka kafka-topics \
  --create \
  --topic stock_quotes \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
```
