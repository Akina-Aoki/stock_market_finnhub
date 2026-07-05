# Docker Pause / Resume + Full Demo Flow

This runbook is for the Finnhub Modern Data Stack project when the local computer is not running all the time.

It covers:

- stopping Docker safely
- starting Docker again after sleep/shutdown
- activating the local Python virtual environment
- checking Airflow on localhost
- handling missed or failed DAG runs
- triggering a fresh ingestion run
- running dbt models and tests
- generating and opening dbt docs
- showing the full project demo flow

Project context:

- Docker Compose runs Airflow locally.
- Airflow orchestrates ingestion, dbt run, and dbt test.
- Snowflake is the warehouse.
- dbt builds staging, intermediate, and mart models.
- The mart layer contains the analytics-ready star schema tables.

---

## 1. Key idea

Because this Airflow setup is local, the pipeline only runs while your computer and Docker containers are running.

So if the computer is asleep, shut down, or Docker is stopped, Airflow is also stopped.

That is fine for this project.

For the demo, the goal is not to pretend that the local machine is always online. The goal is to show that the pipeline can be resumed cleanly and re-run without breaking.

Recommended explanation:

> This is a local Airflow setup using Docker Compose. Since the scheduler only runs when my machine is on, I resume the containers, check the DAG state, and trigger a fresh pipeline run when needed. The ingestion and dbt transformations are designed to be re-runnable.

---

## 2. Recommended DAG configuration for a local project

For a local laptop-based project, use this kind of DAG configuration:

```python
catchup=False
max_active_runs=1
```

Meaning:

| Setting | Why it matters |
|---|---|
| `catchup=False` | Airflow will not try to automatically run every missed schedule while the laptop was off. |
| `max_active_runs=1` | Prevents many overlapping pipeline runs. |

This is better for a local demo because you do not want a backlog of old scheduled runs starting all at once when Docker starts again.

---

## 3. Normal shutdown flow

Use this when you are done working and want to pause your computer.

### Step 1: Go to the project root

```bash
cd ~/de25/stock_market_finnhub
```

### Step 2: Check the latest DAG runs

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

Look at the latest run.

Safe states:

```text
success
failed
```

Avoid stopping if the latest run is:

```text
running
queued
```

If it is running, wait until it finishes if possible.

### Step 3: Stop Docker Compose

```bash
docker compose stop
```

This stops the containers but keeps the state and volumes.

### Step 4: Confirm containers are stopped

```bash
docker ps
```

The Airflow containers should no longer appear in the running container list.

Now you can close VS Code, sleep, restart, or shut down the computer.

---

## 4. Normal resume flow after sleep/shutdown

Use this when you come back and want to continue the project.

### Step 1:  terminal

### Step 2: Go to the project root

```bash
cd ~/de25/stock_market_finnhub
```

### Step 3: Activate the virtual environment

For your Windows Git Bash setup:

```bash
source .venv_stock/Scripts/activate
```

You should see something like this:

```text
(.venv_stock)
```

### Step 4: Start Docker Compose

Before starting containers, make sure Docker Desktop is open and running.

Then check Docker from Git Bash:

`docker ps`

If Docker is working, you should get a table of containers, even if nothing is running.

---

Do not start everything immediately with `docker compose up -d` if the computer was paused or shut down.

#### zookeeper
Do not start everything immediately with `docker compose up -d` if the computer was paused or shut down.

Use this safer order:

`docker compose stop`

Then start Zookeeper first:

`docker compose up -d zookeeper`

Check that Zookeeper is running:

`docker ps`

You should see:

`zookeeper   Up`

---

#### kafka

After Zookeeper is running, start Kafka:

`docker compose up -d kafka`

Check the status:

`docker ps -a`

Kafka should say:

`kafka   Up`

If Kafka says Exited (1), check the logs:

`docker logs kafka --tail=100`

If the log says Kafka timed out waiting for Zookeeper, run:

```
docker compose restart zookeeper

docker compose up -d kafka
```

Then check again:

`docker ps -a`
---

#### Start the full stack

Use this command as the default startup command:

```bash
docker compose up -d
```

This works whether the containers were stopped or need to be recreated.

### Step 5: Check containers

```bash
docker ps
```

You should see Airflow containers such as:

```text
airflow-scheduler
airflow-webserver
airflow-triggerer
airflow-postgres
```

The exact names depend on your `docker-compose.yml`.

---

## 5. Check Airflow localhost

Open this in the browser:

```text
http://localhost:8080
```

---

#### If it is webserver PID file is stale, run:
`docker exec -it airflow-webserver bash -c "rm -f /opt/airflow/airflow-webserver.pid"`

Restart only thr airflow webserver:
`docker compose restart airflow-webserver`

Check the webserver logs again
`docker logs airflow-webserver --tail=80`

Check localhost for airfloe again:
`http://localhost:8080/home`

Check that:

```text
[ ] Airflow UI opens
[ ] finnhub_daily_stock_pipeline appears
[ ] DAG is not broken
[ ] Scheduler is active
[ ] Latest run status is visible
```

---

## 6. Check that the DAG is loaded

From Git Bash:

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list | grep finnhub"
```

Expected result:

```text
finnhub_daily_stock_pipeline
```

If it does not appear, check import errors:

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-import-errors"
```

If it returns:
`finnhub_daily_stock_pipeline | /opt/airflow/dags/02_finnhub_daily_pipeline.py    | airflow | False`

DAG is found by Airflow.
---

## 7. Check latest DAG runs

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

You may see old runs from before the computer was stopped.

This is normal.

Possible cases:

| Situation | What to do |
|---|---|
| Latest run is `success` | Continue normally. |
| Latest run is `failed` because Docker/computer stopped | Trigger a fresh manual run. |
| Latest run is `running` but stuck from before shutdown | Clear or mark it failed in Airflow UI, then trigger a fresh run. |
| Many old scheduled runs appear | Make sure `catchup=False` is set in the DAG. |

---

## 8. If the DAG is paused

Unpause it from the terminal:

```bash
docker exec -it airflow-scheduler bash -c "airflow dags unpause finnhub_daily_stock_pipeline"
```

Or unpause it in the Airflow UI.

---

## 9. Trigger a fresh pipeline run

For resume and demo purposes, the cleanest option is to trigger a new manual run.

```bash
docker exec -it airflow-scheduler bash -c "airflow dags trigger finnhub_daily_stock_pipeline"
```

Then check the run list again:

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

You can also monitor the run in the Airflow UI.

Recommended demo explanation:

> Because this is a local Airflow setup, I trigger a fresh manual run after resuming Docker. This proves that ingestion, dbt transformations, and tests can run end-to-end on demand.

---

## 10. What to do with failed scheduled runs

If the computer was off, some scheduled runs may be missing, delayed, or failed.

For this project, do not over-engineer this.

Recommended rule:

> Keep old run history for transparency, then trigger a fresh manual run to prove the pipeline still works.

Do not delete Airflow metadata just to make the UI look clean.

Do not run:

```bash
docker compose down -v
```

That can remove Docker volumes and delete local Airflow metadata.

---

## 11. Optional: clear a stuck DAG run from the UI

If a previous run is stuck in `running` because the computer shut down mid-run:

1. Open Airflow UI:

```text
http://localhost:8080
```

2. Open `finnhub_daily_stock_pipeline`.
3. Open the stuck DAG run.
4. Clear failed/stuck tasks if you want to re-run them.
5. Or mark the stuck run as failed.
6. Trigger a fresh manual run.

For the demo, the important thing is that the newest manual run succeeds.

---

## 12. Verify dbt locally from the virtual environment

Use this when you want to test dbt from your local terminal, outside Docker.

### Step 1: Make sure venv is active

```bash
source .venv_stock/Scripts/activate
```

### Step 2: Go to the dbt project

```bash
cd ~/de25/stock_market_finnhub/finnhub_stocks
```

### Step 3: Run dbt debug
This checks that dbt can connect to Snowflake.

```bash
dbt debug
```

#### Load snowflake env variable if needed to connect back to Snowflake account:
```
set -a
source ../.env
set +a
```


### Step 4: Run dbt models

```bash
dbt run
```

### Step 5: Run dbt tests

```bash
dbt test
```

---

## 13. Verify only the mart layer

Use this when you specifically want to prove that the final analytics-ready star schema works.

```bash
cd ~/de25/stock_market_finnhub/finnhub_stocks
```

```bash
dbt run --select dim_stock_symbol dim_date fct_stock_quotes_daily
```

``` bash
dbt test --select dim_stock_symbol dim_date fct_stock_quotes_daily
```

Expected result:

``` text
Completed successfully
```

---

## 14. Generate dbt docs

Use this before the full demo.

From the dbt project folder:

Generate docs:

```bash
dbt docs generate
```

Serve docs on port `8081` because Airflow already uses `8080`:

```bash
dbt docs serve --port 8081
```

Open in browser:

```text
http://localhost:8081
```

In the dbt docs demo, show:

```text
[ ] Project lineage graph
[ ] stg_* models
[ ] int_* models
[ ] dim_stock_symbol
[ ] dim_date
[ ] fct_stock_quotes_daily
[ ] model descriptions
[ ] column descriptions
[ ] tests
```

---

## 15. Full demo flow

Use this flow when you want to show the project from start to finish.

### Part A: Start the environment

```bash
cd ~/de25/stock_market_finnhub
```

```bash
source .venv_stock/Scripts/activate
```

```bash
docker compose up -d
```

```bash
docker ps
```

Open Airflow:

```text
http://localhost:8080
```

Show:

```text
[ ] Airflow is running locally
[ ] DAG exists
[ ] DAG has clear task structure
```

---

### Part B: Check previous DAG runs

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

Show that Airflow keeps run history.

Explain:

> Some old scheduled runs may not be perfect because this is a local laptop setup. The important part is that the DAG can be resumed and run successfully on demand.

---

### Part C: Trigger the pipeline

```bash
docker exec -it airflow-scheduler bash -c "airflow dags trigger finnhub_daily_stock_pipeline"
```

Open Airflow UI and show the DAG run.

Show the task order, for example:

```text
ingest_stock_quotes
      ↓
dbt_run
      ↓
dbt_test
```

The exact task names depend on your DAG.

---

### Part D: Show successful run

In Airflow UI, show:

```text
[ ] DAG run is success
[ ] each task is green
[ ] logs are available
```

From terminal:

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

---

### Part E: Show Snowflake tables

In Snowflake, show the schemas/tables:

```sql
-- SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.STAGING; -- will not show since it's a view tables
-- Use this to show view tables instead
SHOW VIEWS IN SCHEMA FINNHUB_STOCKS_MDS.STAGING;



SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.INTERMEDIATE;   -- will not show since it's a view tables
-- Use this to show view tables instead
SHOW VIEWS IN SCHEMA FINNHUB_STOCKS_MDS.INTERMEDIATE;



-- Marts layer is a table 
SHOW TABLES IN SCHEMA FINNHUB_STOCKS_MDS.MARTS;
```

Then show mart table samples:

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.DIM_STOCK_SYMBOL
LIMIT 10;
```

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.DIM_DATE
LIMIT 10;
```

```sql
SELECT *
FROM FINNHUB_STOCKS_MDS.MARTS.FCT_STOCK_QUOTES_DAILY
LIMIT 10;
```

---

### Part F: Run dbt checks manually

From Git Bash:

```bash
cd ~/de25/stock_market_finnhub/finnhub_stocks
```

```bash
dbt debug
```

```bash
dbt run
```

```bash
dbt test
```

Show that tests pass.

---

### Part G: Show dbt docs

Generate docs:

```bash
dbt docs generate
```

Serve docs:

```bash
dbt docs serve --port 8081
```

Open:

```text
http://localhost:8081
```

Show:

```text
[ ] lineage graph
[ ] staging layer
[ ] intermediate layer
[ ] mart layer
[ ] column documentation
[ ] tests
```

---

### Part H: Explain the mart layer

Use this explanation:

> The mart layer is the final analytics-ready layer. It uses a simple star schema with one fact table and two dimension tables. `fct_stock_quotes_daily` stores the daily stock quote metrics, while `dim_stock_symbol` and `dim_date` provide descriptive context for analysis and dashboards.

Show this structure:

```text
DIM_STOCK_SYMBOL
        |
        | stock_symbol_id
        ↓
FCT_STOCK_QUOTES_DAILY
        ↑
        | date_id
DIM_DATE
```

---

## 16. Exact resume command sequence

Use this when you just want the practical command flow.

```bash
cd ~/de25/stock_market_finnhub
```

```bash
source .venv_stock/Scripts/activate
```

```bash
docker compose up -d
```

```bash
docker ps
```

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list | grep finnhub"
```

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

```bash
docker exec -it airflow-scheduler bash -c "airflow dags unpause finnhub_daily_stock_pipeline"
```

```bash
docker exec -it airflow-scheduler bash -c "airflow dags trigger finnhub_daily_stock_pipeline"
```

Open:

```text
http://localhost:8080
```

Then, for dbt docs:

```bash
cd ~/de25/stock_market_finnhub/finnhub_stocks
```

```bash
dbt docs generate
```

```bash
dbt docs serve --port 8081
```

Open:

```text
http://localhost:8081
```

---

## 17. Exact shutdown command sequence

Use this when you are done.

```bash
cd ~/de25/stock_market_finnhub
```

```bash
docker exec -it airflow-scheduler bash -c "airflow dags list-runs -d finnhub_daily_stock_pipeline"
```

If nothing important is running:

```bash
docker compose stop
```

Confirm:

```bash
docker ps
```

Then close the computer.

---

## 18. Commands to avoid during normal work

Do not use this for normal pause/resume:

```bash
docker compose down
```

Do not use this unless you intentionally want to remove Docker volumes:

```bash
docker compose down -v
```

For normal laptop pause/resume, use:

```bash
docker compose stop
```

Then later:

```bash
docker compose up -d
```

---

## 19. Clean demo checklist

```text
[ ] Open Git Bash
[ ] cd ~/de25/stock_market_finnhub
[ ] source .venv_stock/Scripts/activate
[ ] docker compose up -d
[ ] docker ps
[ ] Open Airflow: http://localhost:8080
[ ] Confirm DAG exists
[ ] Confirm DAG is unpaused
[ ] Check previous DAG runs
[ ] Trigger fresh manual DAG run
[ ] Show successful DAG run and task logs
[ ] Show Snowflake raw/staging/intermediate/marts tables
[ ] Run dbt debug
[ ] Run dbt run
[ ] Run dbt test
[ ] Generate dbt docs
[ ] Open dbt docs: http://localhost:8081
[ ] Show dbt lineage graph
[ ] Show mart star schema
[ ] Show tests and documentation
[ ] Stop Docker with docker compose stop
```

---

## 20. How to explain failed or missed runs professionally

Use this wording in the README or presentation:

> Since this project uses a local Airflow instance through Docker Compose, scheduled runs only execute while the local machine is active. To make the workflow reliable for development and demo purposes, the DAG is designed to be re-runnable, and I use manual triggers after resuming Docker. This keeps the pipeline transparent while still demonstrating the full orchestration flow: ingestion, transformation, testing, and documentation.

