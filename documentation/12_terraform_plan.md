## Terraform Infrastructure as Code Validation

Terraform was added as an optional Infrastructure as Code layer for this project. The goal is to define and manage the main cloud infrastructure from code instead of relying only on manual setup.

In this project, Terraform manages the following resources:

* Amazon S3 bucket used for raw Finnhub stock quote storage
* Snowflake database used for the data warehouse
* Snowflake warehouse used for compute
* Snowflake schemas for the pipeline layers: `RAW`, `STAGING`, `INTERMEDIATE`, and `MARTS`

After applying and validating the Terraform configuration, Terraform was aligned with the real project infrastructure:

* S3 bucket: `finnhub-stocks`
* Snowflake database: `FINNHUB_STOCKS_MDS`
* Snowflake warehouse: `FINNHUB_STOCKS_MDS_WH`
* Snowflake schemas: `RAW`, `STAGING`, `INTERMEDIATE`, `MARTS`

The final Terraform validation showed:

```text
No changes. Your infrastructure matches the configuration.
```

This confirms that the infrastructure defined in Terraform matches the actual resources in AWS and Snowflake. Terraform can now be used to recreate or manage the cloud setup in a controlled and repeatable way.

---

## Role of Terraform in the Workflow

Terraform does not replace Docker, Airflow, or dbt. Each tool has a different responsibility in the project.

Docker is used to run the local development environment and services, such as Airflow, Kafka, Zookeeper, and other containers needed by the pipeline.

Terraform is used to manage the cloud infrastructure. In this project, it defines the S3 bucket and Snowflake resources, including the database, warehouse, and schemas. This makes the cloud setup repeatable and easier to recreate from code.

Airflow is responsible for orchestrating the pipeline. It runs the ingestion task, triggers the dbt transformations, and runs dbt tests as part of the DAG workflow.

dbt is responsible for transforming the data inside Snowflake. It builds the staging, intermediate, and mart models, applies data quality tests, and generates documentation for the transformed models.

In short, Terraform prepares the cloud infrastructure, Docker runs the local services, Airflow controls the pipeline execution, and dbt transforms and tests the data.
