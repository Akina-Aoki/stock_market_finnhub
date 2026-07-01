/*
Dimension table for dates.

Input:
INTERMEDIATE.INT_STOCK_QUOTE_METRICS

Output:
MARTS.DIM_DATE

This model creates one row per fetched date.
It helps dashboards filter by year, month, and day.

Grain:
One row per fetched date.
*/


WITH dates AS (

    SELECT DISTINCT
        fetched_date AS calendar_date
    FROM {{ ref('int_stock_quote_metrics') }}

)

SELECT
    -- Date key in YYYYMMDD format.
    -- Example: 2026-06-29 becomes 20260629.
    TO_NUMBER(TO_CHAR(calendar_date, 'YYYYMMDD')) AS date_id,

    -- Actual calendar date.
    calendar_date,

    -- Year from the date.
    YEAR(calendar_date) AS year,

    -- Month number from the date.
    MONTH(calendar_date) AS month,

    -- Month name from the date.
    TRIM(TO_CHAR(calendar_date, 'MMMM')) AS month_name,

    -- Day number inside the month.
    DAY(calendar_date) AS day

FROM dates