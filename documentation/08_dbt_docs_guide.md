## How to open dbt docs locally
`dbt docs generate` builds the documentation files.

`dbt docs serve --port 8081` opens the documentation website locally.


Airflow uses `localhost:8080`, so dbt docs should be served on port `8081`.

### 1. Activate the virtual environment if not activated and dbt local folder

From the project root:

```bash
source .venv_stock/Scripts/activate
cd finnhub_stocks
```

### Check dbt version used

```bash
which dbt
```

Expect:
`/c/Users/adelo/de25/stock_market_finnhub/.venv_stock/Scripts/dbt`

If not correct:
```bash
cd ..
source .venv_stock/Scripts/activate
cd finnhub_stocks
which dbt
dbt --version
```

## Generate dbt docs
Run this when the dbt models or schema.yml documentation has changed:
```bash
dbt docs generate
```

## Serve dbt docs on port 8081
```bash
dbt docs serve --port 8081
```

## Open dbt docs in browser
```bash
http://localhost:8081
```

## Stop the dbt docs server

In the terminal, press:

```bash
CTRL + C
```



```bash

```

```bash

```