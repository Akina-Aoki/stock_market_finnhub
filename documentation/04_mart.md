# 07 Mart Layer Star Schema Plan

This is the revised plan for the mart layer of the Finnhub stock quote project.

The goal of the mart layer is to create clean, analytics-ready tables. This is similar to the Gold layer in the Databricks project. The staging and intermediate layers can still keep technical details, but the mart layer should be easier to use for analysis, dashboards, and explanation.

---

## Pipeline Schedule Decision

For the MVP version of this project, the pipeline is designed to run **once per weekday after the U.S. stock market closes**.

The reason for this is that the mart layer uses a **daily grain**, meaning:

> One row in the fact table represents one stock symbol on one fetched date.

Because of this, the final mart does not need hourly or real-time stock records. Instead, it keeps the latest quote collected for each stock on each day.

The planned Airflow schedule is:

```bash
schedule="15 21 * * 1-5"
```

---

## 1. Main decision: daily mart grain

For the mart layer, we will use a **daily grain**.

This means:

> One row in the fact table = one stock symbol on one fetched date.

Example:

| symbol | fetched_date | meaning |
|---|---:|---|
| AAPL | 2026-06-29 | latest AAPL quote collected on this date |
| MSFT | 2026-06-29 | latest MSFT quote collected on this date |

Because the mart is daily, we will **not keep fetched hour** in the mart table.

The detailed timestamp and hour columns can stay in the intermediate layer, but the final mart should be simpler.

---

## 2. Why daily grain makes sense for this project

Right now, the producer can fetch data many times, but the final project does not need to act like Yahoo Finance or a real-time trading platform.

This project is mainly a modern data stack project. It should show:

- data ingestion
- raw storage
- Snowflake loading
- dbt transformations
- data quality tests
- star schema modeling
- analytics-ready output

For this reason, a daily stock quote mart is enough and easier to explain.

Later, if the pipeline is scheduled once per day with Airflow, this daily grain will also make more sense.

---

## 3. What happens if there are many records per stock per day?

Because the current pipeline may collect several quotes on the same day, we need a rule.

The rule will be:

> For each stock symbol and fetched date, keep the latest quote based on `fetched_at`.

Example:

| symbol | fetched_date | fetched_at | keep? |
|---|---:|---:|---|
| AAPL | 2026-06-29 | 06:11:39 | no |
| AAPL | 2026-06-29 | 06:12:11 | yes |

This creates a clean daily fact table.

---

## 4. Star schema design

The mart layer will have two dimension tables and one fact table.

```text
dim_stock_symbol
        |
        | stock_symbol_id
        ↓
fct_stock_quotes_daily
        ↑
        | date_id
dim_date
```

This is a simple star schema.

The fact table stores the measurable stock quote values.

The dimension tables store descriptive information used for filtering and grouping.

---

## 5. Conceptual model

At a high level, the model is about this:

> A stock has a daily quote on a specific date.

Entities:

| Entity | Meaning |
|---|---|
| Stock symbol | The stock/company ticker, such as AAPL or MSFT |
| Date | The date when the quote was fetched |
| Stock quote daily fact | The daily stock quote values and calculated metrics |

Relationships:

| Relationship | Meaning |
|---|---|
| One stock symbol can have many daily quote records | AAPL can appear on many dates |
| One date can have many stock quote records | Many symbols can be fetched on the same date |
| Each fact row belongs to one stock symbol and one date | This creates the star schema |

---

## 6. Logical model

### `dim_stock_symbol`

One row per stock symbol.

| Column | Description | Test |
|---|---|---|
| `stock_symbol_id` | Generated unique ID for each stock symbol | `not_null`, `unique` |
| `symbol` | Stock ticker symbol, such as AAPL, MSFT, TSLA | `not_null`, `unique` |

Later, this table can be extended with company name, exchange, sector, or industry.

---

### `dim_date`

One row per fetched date.

This dimension will be created from `fetched_date` in the intermediate model.

| Column | Description | Test |
|---|---|---|
| `date_id` | Date key in `YYYYMMDD` format. Example: `20260629` | `not_null`, `unique` |
| `date_day` | Actual date. Example: `2026-06-29` | `not_null`, `unique` |
| `year` | Year from the date |  |
| `month` | Month number |  |
| `month_name` | Month name |  |
| `day_of_month` | Day of month |  |
| `day_of_week` | Day number of the week |  |
| `day_name` | Day name, such as Monday |  |

Why this table is useful:

- It makes the star schema clearer.
- It lets dashboards filter by year, month, day, and weekday.
- It avoids repeating date logic in every fact table.

---

### `fct_stock_quotes_daily`

One row per stock symbol per fetched date.

| Column | Description | Test |
|---|---|---|
| `stock_quote_id` | Generated unique ID for each daily stock quote row | `not_null`, `unique` |
| `stock_symbol_id` | Foreign key to `dim_stock_symbol` | `not_null`, `relationships` |
| `date_id` | Foreign key to `dim_date` | `not_null`, `relationships` |
| `current_price` | Latest current price for that stock/date | `not_null` |
| `price_change` | Price change compared to previous close | `not_null` |
| `price_change_percent` | Price change percentage | `not_null` |
| `high_price` | High price for the trading day | `not_null` |
| `low_price` | Low price for the trading day | `not_null` |
| `open_price` | Opening price for the trading day | `not_null` |
| `previous_close_price` | Previous close price | `not_null` |
| `daily_price_range` | `high_price - low_price` | `not_null` |
| `daily_price_range_percent` | Daily range as a percentage of previous close | `not_null` |
| `price_movement_direction` | `up`, `down`, or `unchanged` | `not_null`, `accepted_values` |
| `price_position_in_daily_range` | Shows where the current price sits between low and high | `not_null` |

---

## 7. Columns we will not include in the mart fact table

These columns are useful for debugging and lineage, but they are not needed in the main analytics-ready mart.

| Column | Why we remove it from marts |
|---|---|
| `source_file_name` | Too technical for the final analytics table |
| `loaded_at` | Pipeline metadata, not stock analysis |
| `loaded_date` | Pipeline metadata, not stock analysis |
| `loaded_hour` | Pipeline metadata, not stock analysis |
| `finnhub_timestamp` | Raw API timestamp, not needed for the daily mart |
| `fetched_at` | Too detailed for daily grain |
| `fetched_hour` | Removed because the mart is daily, not hourly |

These columns can stay in staging and intermediate models.

If we later want a pipeline freshness dashboard, we can create a separate mart for pipeline monitoring.

---

## 8. Rounding decision

For the mart layer, we will round numeric values to make the table more analytics-ready.

This is a conscious trade-off.

### Pros

- Easier to read in Snowflake and dashboards
- Similar to how stock prices are normally displayed
- Better for presentation and final reporting
- Makes the final mart less messy

### Cons

- Slight loss of precision
- Not ideal for advanced financial calculations
- If we need exact values later, we should use intermediate or raw precise columns

Decision:

> Keep precise values in staging and intermediate. Round values in the mart layer because the mart is the final analytics-ready table.

Recommended rounding:

| Column type | Rounding |
|---|---:|
| Price columns | 2 decimals |
| Percentage columns | 2 decimals |
| `price_position_in_daily_range` | 4 decimals |

Example:

| Column | Example before | Example in mart |
|---|---:|---:|
| `price_change_percent` | `5.7081` | `5.71` |
| `daily_price_range_percent` | `6.002890911` | `6.00` |
| `price_position_in_daily_range` | `0.8281397545` | `0.8281` |

Important note:

`price_change_percent = 5.71` means `5.71%`.

We do not store the `%` symbol in Snowflake because then the column becomes text. The dashboard can display the `%` symbol.

---

## 9. Physical model

Snowflake mart tables:

```text
FINNHUB_STOCKS_MDS.MARTS.DIM_STOCK_SYMBOL
FINNHUB_STOCKS_MDS.MARTS.DIM_DATE
FINNHUB_STOCKS_MDS.MARTS.FCT_STOCK_QUOTES_DAILY
```

dbt model files:

```text
finnhub_stocks/models/marts/dim_stock_symbol.sql
finnhub_stocks/models/marts/dim_date.sql
finnhub_stocks/models/marts/fct_stock_quotes_daily.sql
finnhub_stocks/models/marts/schema.yml
```

Because the mart layer is final and analytics-ready, these models should be materialized as tables.

---

## 10. Tests needed for the mart layer

The PDF says the mart layer should include tests such as `not_null`, `unique`, and relationships where applicable.

We will include:

### `dim_stock_symbol`

| Column | Tests |
|---|---|
| `stock_symbol_id` | `not_null`, `unique` |
| `symbol` | `not_null`, `unique` |


### `dim_date`

| Column | Tests |
|---|---|
| `date_id` | `not_null`, `unique` |
| `calendar_date` | `not_null`, `unique` |
| `year` | `not_null` |
| `month` | `not_null` |
| `month_name` | `not_null` |
| `day` | `not_null` |
### `fct_stock_quotes_daily`

| Column | Tests |
|---|---|
| `stock_quote_id` | `not_null`, `unique` |
| `stock_symbol_id` | `not_null`, relationship to `dim_stock_symbol.stock_symbol_id` |
| `date_id` | `not_null`, relationship to `dim_date.date_id` |
| `symbol` | `not_null` |
| `fetched_date` | `not_null` |
| `current_price` | `not_null` |
| `price_movement_direction` | `not_null`, accepted values: `up`, `down`, `unchanged` |

### Extra custom tests for `fct_stock_quotes_daily`

In addition to the standard dbt tests, we added custom SQL tests for business rules and grain validation.

| Test file | Purpose |
|---|---|
| `fct_non_negative_prices.sql` | Checks that price-related columns are not negative. This excludes `price_change` and `price_change_percent` because they can be negative when a stock goes down. |
| `fct_high_price_greater_than_low_price.sql` | Checks that `high_price` is never lower than `low_price`. |
| `fct_price_position_range.sql` | Checks that `price_position_in_daily_range` stays between `0` and `1`. |
| `unique_stock_date.sql` | Checks that the fact table keeps the correct grain: one row per stock symbol per fetched date. |

These custom tests help validate that the mart table is not only technically correct, but also logically correct for stock quote analysis.

---

## 11. Dashboard ideas from the mart layer

The dashboard can use the mart tables to show:

- latest daily quote per stock
- current price by stock
- price change percentage by stock
- up/down/unchanged movement
- daily price range
- stock comparison by date
- filters for year, month, day, and weekday

This keeps the dashboard simple and aligned with the modeled data.

---

## 12. Build order

We will build the mart layer in this order:

1. `dim_stock_symbol`
2. `dim_date`
3. `fct_stock_quotes_daily`
4. `schema.yml` with mart tests
5. run dbt models and tests
6. check the tables in Snowflake

This keeps the work small and easier to debug.
