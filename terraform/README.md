# Optional Terraform setup for Finnhub infrastructure

This folder contains a beginner-friendly, optional Terraform setup for the Finnhub stock market modern data stack project.

Terraform is only used here for cloud/data-platform infrastructure. Your local Airflow Docker environment remains managed by `docker-compose.yml`, not Terraform.

## Files in this folder

```text
terraform/
├── main.tf                  # AWS S3 and Snowflake resources to create
├── variables.tf             # Input variables and safe defaults
├── outputs.tf               # Useful values printed after apply
├── providers.tf             # Terraform, AWS, and Snowflake provider configuration
├── terraform.tfvars.example # Copy-and-edit example values; no real secrets
└── README.md                # Beginner usage guide
```

## What Terraform creates

This Terraform configuration provisions:

1. One private Amazon S3 bucket for raw Finnhub stock quote files.
2. S3 public access blocking for the raw bucket.
3. S3 server-side encryption for the raw bucket.
4. S3 versioning for safer raw file recovery.
5. One Snowflake database, defaulting to `FINNHUB_STOCKS_MDS`.
6. One cost-controlled Snowflake warehouse, defaulting to `FINNHUB_STOCKS_MDS_WH`, `XSMALL`, auto-resume enabled, and auto-suspend after 60 seconds.
7. Four Snowflake schemas for the project layers:
   - `RAW`
   - `STAGING`
   - `INTERMEDIATE`
   - `MARTS`

## What Terraform does not manage

Terraform intentionally does **not** manage:

- Local Docker containers.
- Airflow webserver, scheduler, metadata database, or DAG execution.
- Kafka containers or local runtime state.
- Finnhub API keys.
- dbt models, dbt runs, or dbt tests.
- Snowflake tables, stages, pipes, grants, users, or roles beyond using the configured role to create the requested objects.
- Historical data already present in S3 or Snowflake.

Keep managing local Airflow with Docker Compose from the repository root, for example:

```bash
docker compose up
```

## How this connects to the rest of the project

After `terraform apply` succeeds:

- The producer/consumer or Airflow ingestion flow can write raw Finnhub JSON/JSONL files to the Terraform-created S3 bucket.
- Snowflake has the database and layered schemas expected by the RAW, STAGING, INTERMEDIATE, and MARTS architecture.
- dbt Core can build models into the Snowflake schemas after your dbt profile points at the Terraform-created database and warehouse.
- Airflow can continue to run locally in Docker and orchestrate ingestion, Snowflake loading, dbt transformations, and dbt tests.

You will still need to align your local `.env`, Airflow connections, Snowflake SQL stage setup, and `finnhub_stocks/profiles.yml` with the names you choose in Terraform.

## Prerequisites

Install or have access to:

- Terraform CLI.
- AWS credentials with permission to create and delete S3 buckets.
- A Snowflake user/role with permission to create databases, warehouses, and schemas.

## Configure values safely

From the repository root:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` and replace placeholder values such as:

```hcl
raw_bucket_name = "your-globally-unique-bucket-name"
snowflake_organization_name = "YOUR_ORG_NAME"
snowflake_account_name      = "YOUR_ACCOUNT_NAME"
snowflake_user              = "YOUR_TERRAFORM_USER"
snowflake_role              = "ACCOUNTADMIN"
```

Do not commit `terraform.tfvars`.

### AWS credentials

Use your normal AWS authentication method. For a simple local test, you can export environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_SESSION_TOKEN="your-session-token-if-needed"
```

### Snowflake credentials

Prefer environment variables for secrets.

Password example:

```bash
export TF_VAR_snowflake_password="your-snowflake-password"
```

Private-key example:

```bash
export TF_VAR_snowflake_private_key_file="/absolute/path/to/snowflake_key.p8"
```

Use either password authentication or private-key authentication based on how your Snowflake user is configured.

## Safe beginner workflow

Run these commands from the `terraform/` folder.

### 1. Initialize Terraform

Downloads the AWS and Snowflake providers.

```bash
terraform init
```

### 2. Format the Terraform files

Keeps Terraform style consistent.

```bash
terraform fmt
```

### 3. Validate the configuration

Checks whether the Terraform files are syntactically valid.

```bash
terraform validate
```

### 4. Preview changes before creating anything

Shows what Terraform plans to create without applying changes.

```bash
terraform plan
```

Read the plan carefully. You should see one S3 bucket, related S3 safety settings, one Snowflake database, one Snowflake warehouse, and four Snowflake schemas.

### 5. Create the infrastructure

Creates the resources shown in the plan.

```bash
terraform apply
```

Terraform asks for confirmation. Type `yes` only when the plan looks correct.

### 6. Destroy the infrastructure to avoid costs

When you are done testing, destroy the resources so you do not keep paying for them.

```bash
terraform destroy
```

Terraform asks for confirmation. Type `yes` only when you are sure you want to remove the resources.

> Important: S3 buckets must be empty before AWS allows deletion. If `terraform destroy` fails because the bucket contains objects, delete the test objects from the bucket first, then run `terraform destroy` again.

## Cost-safety notes

- The Snowflake warehouse defaults to `XSMALL` and auto-suspends after 60 seconds.
- Run `terraform destroy` after experiments if you do not need the infrastructure.
- Avoid uploading large files during testing.
- Use a dedicated test bucket name and test Snowflake database if you are learning Terraform for the first time.

## Suggested first test

For the safest first test:

1. Use a unique throwaway bucket name, such as `yourname-finnhub-raw-dev-20260705`.
2. Keep the default Snowflake names or add a `_DEV` suffix.
3. Run `terraform init`, `terraform fmt`, `terraform validate`, and `terraform plan`.
4. Only run `terraform apply` after you understand the plan.
5. Confirm the bucket, database, warehouse, and schemas exist.
6. Run `terraform destroy` when finished.