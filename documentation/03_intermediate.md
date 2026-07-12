### Layer 2: Intermediate
**This model takes the clean stock quote data from the staging layer and adds a few useful columns for analysis and business use.**

Create `int_stock_quote_metrics.sql`. This add the "brains" to the data:
* Extract `fetched_date` and `fetched_hour`.
* Calculate `daily_price_range` (High minus Low).
* Flag price movement (`up`, `down`, `flat`).
* Add simple QA flags (e.g., `is_current_price_missing`).

Model: `int_stock_quote_metrics`  
Snowflake table/view: `FINNHUB_STOCKS_MDS.INTERMEDIATE.INT_STOCK_QUOTE_METRICS`  
Input model: `STAGING.STG_STOCK_QUOTES`



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
| `fetched_date` | The date part of `fetched_at`. | `CAST(fetched_at AS DATE)` |
| `fetched_hour` | The hour part of `fetched_at`. | `DATE_TRUNC('hour', fetched_at)` |
| `daily_price_range` | The difference between the daily high and daily low. | `high_price - low_price` |
| `daily_price_range_percent` | The daily high-low range compared to the previous close. | `((high_price - low_price) / NULLIF(previous_close_price, 0)) * 100` |
| `price_movement_direction` | A simple label showing if the stock went `up`, `down`, or stayed `unchanged`. | Based on `price_change`. |
| `price_position_in_daily_range` | Shows where the current price is between the daily low and high. | `(current_price - low_price) / NULLIF(high_price - low_price, 0)` |


## Calculated columns

### `daily_price_range`

```sql
high_price - low_price
```

How much the stock moved between its lowest and highest price of the day.

---

### `daily_price_range_percent`

```sql
((high_price - low_price) / NULLIF(previous_close_price, 0)) * 100
```

Daily range as a percentage.

This is useful because different stocks have different prices. A `10` dollar movement means something different for a `100` dollar stock compared to a `1000` dollar stock.

`NULLIF(previous_close_price, 0)` is there to avoid dividing by zero.

---

### `price_movement_direction`

```sql
CASE
    WHEN price_change > 0 THEN 'up'
    WHEN price_change < 0 THEN 'down'
    ELSE 'unchanged'
END
```

This makes the data easier to read.

Instead of only seeing numbers like `5.68` or `-6.32`, we also get a simple label:

```text
up
down
unchanged
```

Dashboarding use

---

### `price_position_in_daily_range`

```sql
(current_price - low_price) / NULLIF(high_price - low_price, 0)
```

This shows where the current price is inside the daily range.

| Value | Easy meaning |
|---|---|
| Close to `0` | Current price is near the daily low. |
| Close to `1` | Current price is near the daily high. |
| Around `0.5` | Current price is around the middle of the daily range. |

Example:

```text
0.84 means the current price is closer to the daily high.
```