# dbt Architecture

Think of this in three layers, moving from raw data to dashboard-ready metrics.

| dbt Layer | Prefix | Purpose | Example Model |
|---|---|---|---|
| **Staging** | `stg_` | **Clean:** Rename columns, cast types, extract JSON. No math. | `stg_stock_quotes` |
| **Intermediate** | `int_` | **Calculate:** Add business logic, dates, and math. | `int_stock_quote_metrics` |
| **Marts** | `dim_`, `fct_` | **Analyze:** Final tables for dashboards and reporting. | `fct_stock_quotes` |


## Pipeline

### Layer 1: Staging
`stg_stock_quotes.sql`
* extracts JSON fields
* renames columns into clean snake_case
* casts data types
* keeps metadata columns like source_file_name and loaded_at
* creates the result in Snowflake STAGING schema

`finnhub_stocks/models/staging/schema.yml`
* dbt tests and documentation 

`schema.yml` 
* documents the model and columns
* runs 12 not_null tests

`tests/non_negative_stock_price.sql`
* checks that stock prices are not negative

`tests/high_price_greater_than_low_price.sql`
* checks that high_price >= low_price


## Testing & Documentation Checklist

Add `schema.yml` files to ensure data quality:

**Tests:** 
* Enforce `not_null` on critical columns (symbols, timestamps, prices). 
  * Enforce `unique` on your dimension symbols. 
  * Ensure a relationship test exists linking your fact tables to your dimension table.

* **Docs:** Add a 1-2 sentence description for every model and critical column so your dbt docs generate cleanly.