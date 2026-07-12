## Terraform Infrastructure as Code (Show terraform_plan image)

Terraform was added as an optional Infrastructure as Code layer for this project.

Its role is to define the main cloud resources in code, instead of creating everything manually.

In this project, Terraform manages:

- S3 bucket: `finnhub-stocks`
- Snowflake database: `FINNHUB_STOCKS_MDS`
- Snowflake warehouse: `FINNHUB_STOCKS_MDS_WH`
- Snowflake schemas: `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`

After validation, Terraform showed:

```text
No changes. Your infrastructure matches the configuration.
```

This means the Terraform code matches the real AWS and Snowflake setup.

----

Terraform does not replace Docker, Airflow, or dbt.

Each tool has a different role:

- **Terraform** creates and manages cloud infrastructure.
- **Docker** runs the local services, such as Airflow, Kafka, and Zookeeper.
- **Airflow** controls and schedules the pipeline.
- **dbt** transforms and tests the data inside Snowflake.

In short:

```text
Terraform = prepares cloud infrastructure
Docker = runs local services
Airflow = orchestrates the workflow
dbt = transforms and tests the data
```
