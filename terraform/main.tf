# S3 bucket for raw Finnhub quote files before they are loaded into Snowflake.
# Bucket names must be globally unique across all AWS accounts.
resource "aws_s3_bucket" "raw_finnhub_quotes" {
  bucket = var.raw_bucket_name
}

# Keep the raw data bucket private. Airflow/producer credentials should access it with IAM,
# not with public bucket permissions.
resource "aws_s3_bucket_public_access_block" "raw_finnhub_quotes" {
  bucket = aws_s3_bucket.raw_finnhub_quotes.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enable bucket encryption at rest with Amazon S3-managed keys (SSE-S3).
resource "aws_s3_bucket_server_side_encryption_configuration" "raw_finnhub_quotes" {
  bucket = aws_s3_bucket.raw_finnhub_quotes.id

  rule {
    bucket_key_enabled = true

    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Enable versioning so accidental overwrites/deletes of raw files are easier to recover from.
resource "aws_s3_bucket_versioning" "raw_finnhub_quotes" {
  bucket = aws_s3_bucket.raw_finnhub_quotes.id

  versioning_configuration {
    status = "Disabled"
  }
}

# Snowflake database that contains all dbt layers for the Finnhub project.
resource "snowflake_database" "finnhub" {
  name = var.snowflake_database_name
}

# Cost-controlled Snowflake virtual warehouse for loading and transforming project data.
resource "snowflake_warehouse" "finnhub" {
  name                = var.snowflake_warehouse_name
  warehouse_size      = var.snowflake_warehouse_size
  auto_suspend        = var.snowflake_auto_suspend_seconds
  auto_resume         = true
  initially_suspended = true

  enable_query_acceleration           = true
  query_acceleration_max_scale_factor = 2

  min_cluster_count = 1
  max_cluster_count = 1
  scaling_policy    = "STANDARD"
  warehouse_type    = "STANDARD"

  lifecycle {
    ignore_changes = [
      generation,
    ]
  }
}

# Create one schema per data layer: RAW, STAGING, INTERMEDIATE, and MARTS.
resource "snowflake_schema" "layers" {
  for_each = var.snowflake_schemas

  database = snowflake_database.finnhub.name
  name     = each.value

  is_transient        = false
  with_managed_access = false
}