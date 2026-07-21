# TimescaleDB Migration Progress

## Project goal

The Finnhub project originally used Snowflake as the warehouse.

The pipeline was:

```text
Finnhub API
    ↓
Kafka producer
    ↓
Kafka consumer
    ↓
Amazon S3 JSONL files
    ↓
Snowflake RAW layer
    ↓
dbt staging
    ↓
dbt intermediate
    ↓
dbt marts
```

Snowflake is being replaced because the free trial is ending and the project should not create unexpected costs.

TimescaleDB was chosen because:

- it can run locally with Docker
- it is based on PostgreSQL
- it is suitable for timestamped data
- stock quotes are time-series data
- it can be used without paying for a managed cloud warehouse

Amazon S3 is still used. The Finnhub API, Kafka producer, Kafka consumer, and S3 raw storage have not been removed.

The planned pipeline is now:

```text
Finnhub API
    ↓
Kafka producer
    ↓
Kafka consumer
    ↓
Amazon S3 JSONL files
    ↓
Airflow loader
    ↓
TimescaleDB RAW layer
    ↓
dbt staging
    ↓
dbt intermediate
    ↓
dbt marts
```

The Airflow loader and dbt migration are not complete yet.

---

## What each tool does

It is important to separate Docker, TimescaleDB, and DBeaver.

```text
Docker      = runs TimescaleDB on the local computer
TimescaleDB = stores and queries the stock data
DBeaver     = visual interface used to browse and query TimescaleDB
```

Snowflake combined the cloud database and browser interface in one service.

The local setup is more modular:

```text
Snowflake cloud warehouse  → TimescaleDB
Snowflake web interface    → DBeaver
Snowflake cloud server     → Docker on the local computer
```

Closing DBeaver does not stop TimescaleDB.

Stopping the TimescaleDB Docker container does not delete the database because the data is stored in a persistent Docker volume.

---

## Security work completed

During setup, `docker compose config` printed the resolved environment variables in the terminal.

This exposed secret values in the command output.

The following credentials were rotated:

- AWS access key and secret key
- Finnhub API key
- Snowflake password
- TimescaleDB admin password
- TimescaleDB loader password
- TimescaleDB dbt password

The private `.env` file remains ignored by Git and was not committed.

### Important rule

Use this command to validate Docker Compose:

```bash
docker compose config --quiet
```

Do not use this when real secrets are loaded:

```bash
docker compose config
```

The normal command prints the resolved configuration and may display passwords and API keys.

Never paste `.env` contents, API keys, passwords, or AWS secret keys into documentation, GitHub, screenshots, or chat messages.

---

## Environment variables

The private `.env` keeps the existing Finnhub, Kafka, AWS, and temporary Snowflake settings.

It also contains the new TimescaleDB settings:

```dotenv
TIMESCALEDB_DATABASE=finnhub_stocks_mds
TIMESCALEDB_ADMIN_USER=finnhub_admin
TIMESCALEDB_ADMIN_PASSWORD=
TIMESCALEDB_HOST_PORT=5433

TIMESCALEDB_LOADER_USER=finnhub_loader
TIMESCALEDB_LOADER_PASSWORD=

TIMESCALEDB_DBT_USER=finnhub_dbt
TIMESCALEDB_DBT_PASSWORD=
```

Real passwords must only exist in the private `.env`.

The Snowflake settings are temporarily kept because the old Airflow DAG and dbt project still depend on Snowflake. They should only be removed after the TimescaleDB pipeline works end to end.

---

## Two separate PostgreSQL databases

The project now has two PostgreSQL-based services.

| Service | Purpose | Connection from Windows | Connection inside Docker |
| --- | --- | --- | --- |
| Airflow PostgreSQL | Stores Airflow metadata, task status, DAG runs, and Airflow configuration | `localhost:5432` | `postgres:5432` |
| TimescaleDB | Stores Finnhub stock quote data and future dbt models | `localhost:5433` | `timescaledb:5432` |

These databases must stay separate.

Do not store stock quote data in the Airflow metadata database.

---

## Docker setup completed

A new TimescaleDB service was added to `docker-compose.yml`.

The service uses:

```text
Image: timescale/timescaledb:2.17.2-pg16
Container: finnhub-timescaledb
Host port: 5433
Container port: 5432
Database: finnhub_stocks_mds
Persistent volume: stock_market_finnhub_timescaledb_data
```

The container was started and confirmed healthy:

```bash
docker compose up -d timescaledb
docker compose ps timescaledb
```

Expected status:

```text
Up ... (healthy)
```

---

## Initialization SQL

The database initialization file is:

```text
sql/timescaledb-init/00_init.sql
```

Docker runs this file automatically only when it creates a new, empty TimescaleDB volume.

The first attempt did not run because Docker mounted:

```text
sql/timescaledb-init
```

but the file was originally stored in:

```text
sql/timescaledb
```

The folder was renamed to match the Docker Compose mount.

Because the volume had already been created, the empty TimescaleDB container and volume were recreated. The initialization SQL then ran successfully.

---

## Database objects created

The following database was created:

```text
finnhub_stocks_mds
```

The following schemas were created:

```text
raw
staging
intermediate
marts
```

Their purpose is:

| Schema | Purpose |
| --- | --- |
| `raw` | Stores data loaded from the original S3 JSONL files |
| `staging` | Cleans column names, data types, and basic source issues |
| `intermediate` | Applies reusable business logic and calculated fields |
| `marts` | Stores analytics-ready dimensions and fact tables |

The same logical dbt layers used in Snowflake will be preserved.

---

## Raw TimescaleDB table

The following table was created:

```text
raw.raw_stock_quotes
```

It is a TimescaleDB hypertable partitioned by:

```text
fetched_at
```

A hypertable is a TimescaleDB table designed for time-series data. TimescaleDB internally organizes the records by time so timestamp-based queries can scale efficiently.

The table contains typed source fields such as:

```text
fetched_at
symbol
current_price
price_change
price_change_percent
high_price
low_price
open_price
previous_close_price
finnhub_timestamp
```

It also contains the original JSON record and pipeline metadata:

```text
raw_record
run_id
source_file_name
source_object_version_id
source_record_number
source_etag
loaded_at
```

### Why the new RAW table has more columns than Snowflake

The Snowflake RAW table mainly stored the full JSON object in a `VARIANT` column, plus file and load metadata.

The dbt staging model then extracted fields such as `symbol`, `current_price`, and `fetched_at`.

The TimescaleDB RAW design stores:

- the original JSON object in `raw_record`
- the important source values as typed columns
- technical loading metadata

This is still a raw layer because no business calculations, daily aggregation, rounding, dimension joins, or latest-record filtering have been applied.

Typed columns are useful because:

- `fetched_at` must be a real timestamp for the hypertable
- fields can be indexed
- validation is easier
- PostgreSQL queries are simpler
- the original JSON is still preserved

---

## Idempotency and duplicate protection

The raw hypertable has a unique constraint:

```text
fetched_at
source_file_name
source_object_version_id
source_record_number
```

This will help the future Airflow loader avoid inserting the same S3 record more than once when a task is retried.

When an S3 object does not have a version ID, the value defaults to:

```text
unversioned
```

This prevents duplicate records from bypassing the unique constraint because of null values.

The future loader must use the same source identity when inserting records.

---

## Database users created

Three TimescaleDB users are part of the design.

### `finnhub_admin`

Administrative database user.

Used for:

- database initialization
- schemas
- tables
- roles
- grants
- maintenance

This user should not be used by normal pipeline tasks.

### `finnhub_loader`

Future Airflow loader user.

Permissions:

- connect to `finnhub_stocks_mds`
- use the `raw` schema
- read from `raw.raw_stock_quotes`
- insert into `raw.raw_stock_quotes`

The connection was tested successfully.

The current row count is:

```text
0
```

This is expected because the old S3 stock files have not yet been loaded into TimescaleDB.

### `finnhub_dbt`

Future dbt user.

Permissions:

- connect to `finnhub_stocks_mds`
- read from `raw.raw_stock_quotes`
- create objects in:
  - `staging`
  - `intermediate`
  - `marts`

The permission check confirmed that `finnhub_dbt` can create objects in the `staging` schema.

Neither application user has superuser permissions.

---

## Validation completed

The following checks passed.

### TimescaleDB extension

```text
timescaledb 2.17.2
```

### Project schemas

```text
intermediate
marts
raw
staging
```

### Hypertable

```text
raw.raw_stock_quotes
```

### Table structure

The table contains:

- timestamped Finnhub fields
- JSONB source record
- S3 metadata
- load metadata
- unique retry protection
- indexes for time, symbol, run ID, and source file

### Loader user

The loader user successfully connected and queried:

```sql
select count(*)
from raw.raw_stock_quotes;
```

Result:

```text
0
```

### dbt user

The dbt user successfully connected and confirmed `CREATE` permission on `staging`.

---

## DBeaver setup

DBeaver Community was installed as the visual database interface.

Connection settings:

```text
Database type: PostgreSQL
Host: localhost
Port: 5433
Database: finnhub_stocks_mds
Username: finnhub_admin
Password: value from TIMESCALEDB_ADMIN_PASSWORD
```

The connection test succeeded.

DBeaver now shows:

```text
finnhub_stocks_mds
└── Schemas
    ├── raw
    ├── staging
    ├── intermediate
    └── marts
```

The internal schemas beginning with `_timescaledb_` belong to TimescaleDB and can normally be ignored.

The table is visible here:

```text
raw
└── Tables
    └── raw_stock_quotes
```

The Properties tab displays the columns, indexes, owner, and constraints.

The Data tab is empty because no S3 records have been loaded yet.

DBeaver does not store the data. It only connects to and displays the TimescaleDB database running in Docker.

---

## Current status

Completed:

- rotated exposed credentials
- protected the private `.env`
- added TimescaleDB to Docker Compose
- configured port `5433`
- created a persistent database volume
- enabled the TimescaleDB extension
- created four project schemas
- created `raw.raw_stock_quotes`
- converted the raw table into a hypertable
- created indexes and duplicate protection
- created loader and dbt users
- granted minimum required permissions
- tested loader and dbt access
- installed DBeaver
- connected DBeaver to TimescaleDB
- confirmed the old S3 files still exist

Not completed yet:

- S3-to-TimescaleDB backfill
- replacement of `copy_s3_to_snowflake`
- Airflow `load_s3_to_timescaledb` task
- PostgreSQL Python driver migration
- dbt adapter migration from `dbt-snowflake` to `dbt-postgres`
- dbt SQL conversion
- staging, intermediate, and mart model creation in TimescaleDB
- end-to-end Airflow test
- Snowflake Terraform cleanup
- final documentation and architecture diagrams

---

## Next planned phase

The next phase is to move the existing JSONL stock quote files from S3 into the TimescaleDB raw hypertable.

```text
Existing Amazon S3 JSONL files
        ↓
Backfill loader
        ↓
raw.raw_stock_quotes
        ↓
Visible in DBeaver
```

After the backfill is tested, the main Airflow task will change from:

```text
copy_s3_to_snowflake
```

to:

```text
load_s3_to_timescaledb
```

The loader must:

- use the Airflow `run_id`
- find the correct S3 object
- read and validate the JSONL records
- insert the batch inside one transaction
- use the `finnhub_loader` database user
- avoid duplicates on retries
- fail clearly when a batch is missing or invalid
- return the inserted row count

---

## Start, stop, and inspect TimescaleDB

### Start

```bash
docker compose up -d timescaledb
```

### Check health

```bash
docker compose ps timescaledb
```

### View recent logs

```bash
docker compose logs --tail=100 timescaledb
```

### Stop without deleting data

```bash
docker compose stop timescaledb
```

### Start again

```bash
docker compose up -d timescaledb
```

The persistent volume keeps the database between restarts.

---

## Important warning about deleting the volume

This command permanently deletes the local TimescaleDB database:

```bash
docker volume rm stock_market_finnhub_timescaledb_data
```

Only remove the volume when intentionally resetting the local database.

Deleting the TimescaleDB volume does not delete the original JSONL files in Amazon S3.

Before deleting a volume, verify the exact name:

```bash
docker volume ls
```

---

## Safe verification commands

Load the local environment without printing it:

```bash
set -a
source .env
set +a
```

Validate Docker Compose without exposing resolved secrets:

```bash
docker compose config --quiet
```

Check the extension:

```bash
docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select extname, extversion from pg_extension where extname = 'timescaledb';"
```

Check the schemas:

```bash
docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select schema_name from information_schema.schemata where schema_name in ('raw', 'staging', 'intermediate', 'marts') order by schema_name;"
```

Check the table:

```bash
docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "\\d+ raw.raw_stock_quotes"
```

Check hypertable registration:

```bash
docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select hypertable_schema, hypertable_name from timescaledb_information.hypertables;"
```

Check the current number of raw rows:

```bash
docker compose exec \
  -e PGPASSWORD="$TIMESCALEDB_LOADER_PASSWORD" \
  timescaledb \
  psql -U "$TIMESCALEDB_LOADER_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select count(*) from raw.raw_stock_quotes;"
```

---

## Resume point

The TimescaleDB foundation is working and is visible in DBeaver.

The next task is:

> Build and test a safe S3-to-TimescaleDB backfill loader before changing the main Airflow DAG.