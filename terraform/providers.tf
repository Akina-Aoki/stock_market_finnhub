terraform {
  required_version = ">= 1.5.0"

  required_providers {
    # AWS provider provisions the S3 bucket that stores raw Finnhub quote files.
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }

    # Snowflake provider provisions the database, warehouse, and schemas.
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

# AWS credentials are intentionally not hardcoded here.
# For local testing, authenticate with environment variables such as:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and optionally AWS_SESSION_TOKEN.
provider "aws" {
  region = var.aws_region
}

# Snowflake credentials are intentionally not hardcoded here.
# Prefer environment variables for secrets, for example:
# TF_VAR_snowflake_password or TF_VAR_snowflake_private_key_file.
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  role              = var.snowflake_role
  password          = var.snowflake_password
}