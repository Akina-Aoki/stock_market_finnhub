###  ✅ Layer 2: Intermediate (dbt docs int_stock_quote_metrics)
**This model takes the clean stock quote data from the staging layer and adds a few useful calculate columns for analysis and business use.**



 ✅ (Show snowflake 08_intermediate.sql)
 Create `int_stock_quote_metrics.sql`. 

 | Column | Description | Source / Calculation |
|---|---|---|
| `fetched_date` | The date part of `fetched_at`. | `CAST(fetched_at AS DATE)` |
| `fetched_hour` | The hour part of `fetched_at`. | `DATE_TRUNC('hour', fetched_at)` |
| `daily_price_range` | The difference between the daily high and daily low. | `high_price - low_price` |
| `daily_price_range_percent` | The daily high-low range compared to the previous close. | `((high_price - low_price) / NULLIF(previous_close_price, 0)) * 100` |
| `price_movement_direction` | A simple label showing if the stock went `up`, `down`, or stayed `unchanged`. | Based on `price_change`. |
| `price_position_in_daily_range` | Shows where the current price is between the daily low and high. | `(current_price - low_price) / NULLIF(high_price - low_price, 0)` |


Model: `int_stock_quote_metrics`  
Snowflake table/view: `FINNHUB_STOCKS_MDS.INTERMEDIATE.INT_STOCK_QUOTE_METRICS`  
Input model: `STAGING.STG_STOCK_QUOTES`



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