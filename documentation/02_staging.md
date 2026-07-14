# ✅ dbt Architecture

Moving from raw data to Staging schema.

| dbt Layer | Prefix | Purpose | Example Model |
|---|---|---|---|
| **Staging** | `stg_` | **Clean:** Rename columns, cast types, extract JSON. No math. | `stg_stock_quotes` |
| **Intermediate** | `int_` | **Calculate:** Add business logic, dates, and math. | `int_stock_quote_metrics` |
| **Marts** | `dim_`, `fct_` | **Analyze:** Final tables for dashboards and reporting. | `fct_stock_quotes` |


## ✅ Pipeline (dbt docs stg_stock_quotes)

### Layer 1: Staging
`stg_stock_quotes.sql`
* extracts JSON fields
* renames columns into clean snake_case
* casts data types
* keeps metadata columns like source_file_name and loaded_at
* creates the result in Snowflake STAGING schema

`finnhub_stocks/models/staging/schema.yml`
* dbt tests and documentation 

`schema.yml` (show Data Tests in dbt docs)
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


## ✅ Show Table (Snowflake 07_staging.sql)



| Column | Description | Source / Calculation |
|---|---|---|
| `symbol` | The stock ticker, like `AAPL`, `MSFT`, or `TSLA`. | From staging. |
| `current_price` | The current stock price from Finnhub. | From staging. |
| `price_change` | How much the price changed compared to the previous close. This can be positive or negative. | From staging. |
| `price_change_percent` | The same price movement, but shown as a percentage. This can also be positive or negative. | From staging. |
| `high_price` | The highest price of the stock during the trading day. | From staging. |
| `low_price` | The lowest price of the stock during the trading day. | From staging. |
| `open_price` | The price when the trading day opened. | From staging. |
| `previous_close_price` | The closing price from the previous trading day. | From staging. |
| `finnhub_timestamp` | The timestamp that came from Finnhub. | From staging. |
| `fetched_at` | When our Python producer fetched the quote from the API. | From staging. |
| `source_file_name` | The S3 file where the row originally came from. | From staging. |
| `loaded_at` | When the row was loaded into Snowflake. | From staging. |