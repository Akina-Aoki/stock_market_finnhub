# dbt Transformation Plan

## 1. dbt Architecture

Think of this in three layers, moving from raw data to dashboard-ready metrics.

| dbt Layer | Prefix | Purpose | Example Model |
|---|---|---|---|
| **Staging** | `stg_` | **Clean:** Rename columns, cast types, extract JSON. No math. | `stg_stock_quotes` |
| **Intermediate** | `int_` | **Calculate:** Add business logic, dates, and math. | `int_stock_quote_metrics` |
| **Marts** | `dim_`, `fct_` | **Analyze:** Final tables for dashboards and reporting. | `fct_stock_quotes` |

---

## 2. The Pipeline

### Layer 1: Staging
* `stg_stock_quotes` model is perfectly scoped. 
* extracts JSON fields
* renames columns into clean snake_case
* casts data types
* keeps metadata columns like source_file_name and loaded_at
* creates the result in Snowflake STAGING schema
* dbt tests and documentation `finnhub_stocks/models/staging/schema.yml`

### Layer 2: Intermediate
Create `int_stock_quote_metrics.sql`. This add the "brains" to the data:
* Extract `fetched_date` and `fetched_hour`.
* Calculate `daily_price_range` (High minus Low).
* Flag price movement (`up`, `down`, `flat`).
* Add simple QA flags (e.g., `is_current_price_missing`).

### Layer 3: Marts
Build three simple models here:
1. **`dim_stock_symbol`**: A dimension table built from a simple dbt seed (`stock_symbols.csv` file) containing stock symbols, company names, and sectors.
2. **`fct_stock_quotes`**: Your main fact table, fed directly from your intermediate model. One row per quote snapshot. 
3. **`fct_daily_stock_summary`**: An aggregated table showing the daily highs, lows, and averages per stock. 

---

## 3. Execution Order

Build exact sequence:

1. **Run Staging:** `stg_stock_quotes` (Already working).
2. **Build Intermediate:** `int_stock_quote_metrics` (Add your math and date logic here).
3. **Seed Dimensions:** Create `stock_symbols.csv` and build `dim_stock_symbol`.
4. **Build Facts:** `fct_stock_quotes`.
5. **Build Aggregations:** `fct_daily_stock_summary`.

---

## 4. Testing & Documentation Checklist

Add `schema.yml` files to ensure data quality:

* **Tests:** * Enforce `not_null` on critical columns (symbols, timestamps, prices). 
  * Enforce `unique` on your dimension symbols. 
  * Ensure a relationship test exists linking your fact tables to your dimension table.
* **Docs:** Add a 1-2 sentence description for every model and critical column so your dbt docs generate cleanly.