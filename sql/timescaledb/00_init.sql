-- Initialize the local TimescaleDB analytics database on first volume creation.
-- Docker runs this file only when timescaledb_data is empty. Statements are also
-- idempotent so they are safe to execute manually for schema verification.

create extension if not exists timescaledb;

create schema if not exists raw;
create schema if not exists staging;
create schema if not exists intermediate;
create schema if not exists marts;

create table if not exists raw.raw_stock_quotes (
    fetched_at timestamptz not null,
    symbol text not null,
    current_price numeric,
    price_change numeric,
    price_change_percent numeric,
    high_price numeric,
    low_price numeric,
    open_price numeric,
    previous_close_price numeric,
    finnhub_timestamp bigint,
    run_id text not null,
    raw_record jsonb not null,
    source_file_name text not null,
    -- An S3 VersionId is nullable for buckets without versioning. Store the
    -- explicit fallback instead so the unique constraint deduplicates retries.
    source_object_version_id text not null default 'unversioned',
    source_record_number integer not null,
    source_etag text,
    loaded_at timestamptz not null default now(),

    -- TimescaleDB requires a hypertable unique key to include fetched_at, its
    -- partitioning column. No surrogate primary key is created for this reason.
    constraint raw_stock_quotes_batch_dedup unique (
        fetched_at,
        source_file_name,
        source_object_version_id,
        source_record_number
    )
);

select create_hypertable(
    'raw.raw_stock_quotes',
    by_range('fetched_at'),
    if_not_exists => true
);

create index if not exists raw_stock_quotes_symbol_fetched_at_idx
    on raw.raw_stock_quotes (symbol, fetched_at desc);

create index if not exists raw_stock_quotes_run_id_idx
    on raw.raw_stock_quotes (run_id);

create index if not exists raw_stock_quotes_source_file_idx
    on raw.raw_stock_quotes (source_file_name, source_object_version_id);

-- Role strategy (do not create login roles/passwords in version-controlled SQL):
-- * Docker POSTGRES_USER creates the administrative finnhub_admin role.
-- * A privileged operator later creates finnhub_loader and finnhub_dbt using
--   secrets from the local environment, then grants the documented privileges.