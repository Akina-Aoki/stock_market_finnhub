flowchart LR
    A[Finnhub API] --> B[Python Kafka Producer]
    B --> C[Kafka Topic: stock_quotes]
    C --> D[Python Kafka Consumer]
    D --> E[S3 JSONL Files]
    E --> F[Snowflake RAW.RAW_STOCK_QUOTES]

    F --> G[dbt Staging View: STAGING.STG_STOCK_QUOTES]
    G --> H[dbt Intermediate View: INTERMEDIATE.INT_STOCK_QUOTE_METRICS]

    H --> I[dbt Mart Table: MARTS.DIM_STOCK_SYMBOL]
    H --> J[dbt Mart Table: MARTS.DIM_DATE]
    H --> K[dbt Mart Table: MARTS.FCT_STOCK_QUOTES_DAILY]

    I --> K
    J --> K