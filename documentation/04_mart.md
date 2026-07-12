## Layer 3: Marts (⭐Show Data Model)
The goal of the mart layer is to create clean, analytics-ready tables. This is similar to the Gold layer in the Databricks project. The staging and intermediate layers can still keep technical details, but the mart layer should be easier to use for analysis, dashboards, and explanation.

**Because the mart layer is final and analytics-ready, these models should be materialized as tables.**


---

### Build three simple models here: ⭐ Star Schema

1. **`dim_stock_symbol`**: A dimension table containing stock symbols.

2. **`fct_stock_quotes`**: Main fact table, fed directly from intermediate model. One row per quote snapshot. 

3. **`dim_date`**: This dimensional table creates one row per fetched date.
It helps dashboards filter by year, month, and day. 
        Grain: One row per fetched date.


## Conceptual model

> A stock has a daily quote on a specific date.

| Entity | Meaning |
|---|---|
| Stock symbol | The stock/company ticker, such as AAPL or MSFT |
| Date | The date when the quote was fetched |
| Stock quote daily fact | The daily stock quote values and calculated metrics |


| Relationship | Meaning |
|---|---|
| One stock symbol can have many daily quote records | AAPL can appear on many dates |
| One date can have many stock quote records | Many symbols can be fetched on the same date |
| Each fact row belongs to one stock symbol and one date | This creates the star schema |

---

## Logical model

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

## Columns we will not include in the mart fact table

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


## Rounding decision

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



## Tests needed for the mart layer

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

