# TimescaleDB Foundation

## Why this service exists

TimescaleDB is the new local analytics database foundation for the Finnhub data
stack. It is PostgreSQL with time-series capabilities. The first TimescaleDB
object is `raw.raw_stock_quotes`, a hypertable partitioned by `fetched_at` so the
project can efficiently retain timestamped quote events. S3 remains the durable,
immutable JSONL source of truth; TimescaleDB is the queryable analytics copy.

## Two separate PostgreSQL services

| Database | Purpose | Host connection | Container connection |
| --- | --- | --- | --- |
| Airflow metadata PostgreSQL | Airflow DAG runs, task status, and metadata | `localhost:5432` | `postgres:5432` |
| TimescaleDB analytics database | Raw and future dbt analytics schemas | `localhost:5433` | `timescaledb:5432` |

Do not use Airflow's metadata database for quote data. The Timescale database is
`finnhub_stocks_mds` and has the lower-case schemas `raw`, `staging`,
`intermediate`, and `marts`.

## Environment variables

Copy `.env.example` to `.env` and replace the safe placeholders locally. The
TimescaleDB foundation needs:

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

`.env` is ignored by Git. Do not commit real API keys, AWS credentials, database
passwords, or Snowflake credentials.

## Start and verify

From the repository root in Git Bash or another shell:

```bash
docker compose config
docker compose up -d timescaledb
docker compose ps
docker compose logs --tail=100 timescaledb
```

The service has a persistent named volume, `timescaledb_data`. Its initialization
SQL is `sql/timescaledb-init/00_init.sql` and runs automatically only when Docker
creates an empty volume.

Load your local environment without printing it, then inspect the extension:

```bash
set -a
source .env
set +a

docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select extname, extversion from pg_extension where extname = 'timescaledb';"
```

Verify schemas, table shape, and hypertable registration:

```bash
docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select schema_name from information_schema.schemata where schema_name in ('raw', 'staging', 'intermediate', 'marts') order by schema_name;"

docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "\\d+ raw.raw_stock_quotes"

docker compose exec timescaledb \
  psql -U "$TIMESCALEDB_ADMIN_USER" -d "$TIMESCALEDB_DATABASE" \
  -c "select hypertable_schema, hypertable_name from timescaledb_information.hypertables;"
```

## Raw table and idempotency

The raw table retains typed Finnhub source fields, original `raw_record`, `run_id`,
S3 source identity, and `loaded_at`. The only initial hypertable is
`raw.raw_stock_quotes`; staging, intermediate, dimensions, and marts will remain
regular PostgreSQL relations when dbt is migrated.

For idempotency, `source_object_version_id` is `not null` with the fallback value
`unversioned`. This avoids PostgreSQL's normal unique-constraint behavior that
allows repeated nulls. The unique key is `(fetched_at, source_file_name,
source_object_version_id, source_record_number)`. It includes `fetched_at`, which
is required because TimescaleDB requires unique keys on hypertables to include the
partitioning column. A future loader will require one S3 JSONL object per Airflow
`run_id` and use this constraint for retry-safe inserts.

## Roles: follow-up command template

The initialization SQL deliberately does not create application login roles,
because passwords must not be committed. Run the following after loading `.env`
and after TimescaleDB is healthy; replace placeholders through psql variables or
your secret manager rather than saving secrets in a file. The Docker
`TIMESCALEDB_ADMIN_USER` is the administrative role (`finnhub_admin` by default).

```sql
create role finnhub_loader login password :'loader_password';
create role finnhub_dbt login password :'dbt_password';

grant connect on database finnhub_stocks_mds to finnhub_loader, finnhub_dbt;
grant usage on schema raw to finnhub_loader, finnhub_dbt;
grant select, insert on raw.raw_stock_quotes to finnhub_loader;
grant select on raw.raw_stock_quotes to finnhub_dbt;
grant usage, create on schema staging, intermediate, marts to finnhub_dbt;
```

Neither application role receives superuser privilege. The exact non-secret
operator procedure can be automated when the loader/dbt migration phases add a
secret-management approach.

## Stop or remove

Stop TimescaleDB without deleting data:

```bash
docker compose stop timescaledb
```

Start it again with `docker compose up -d timescaledb`; the `timescaledb_data`
volume preserves the database. To intentionally delete **all local TimescaleDB
data**, first stop the service and then remove the named volume:

```bash
docker compose stop timescaledb
docker volume rm stock_market_finnhub_timescaledb_data
```

The exact volume prefix may differ if Docker Compose uses a different project
name; run `docker volume ls` to identify it. **Removing this volume permanently
deletes the local TimescaleDB database. It does not delete the immutable S3 raw
files.**