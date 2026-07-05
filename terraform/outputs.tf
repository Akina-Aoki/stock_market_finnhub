output "raw_bucket_name" {
  description = "Name of the S3 bucket for raw Finnhub files."
  value       = aws_s3_bucket.raw_finnhub_quotes.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the S3 bucket for raw Finnhub files."
  value       = aws_s3_bucket.raw_finnhub_quotes.arn
}

output "snowflake_database_name" {
  description = "Snowflake database created for the project."
  value       = snowflake_database.finnhub.name
}

output "snowflake_warehouse_name" {
  description = "Snowflake warehouse created for the project."
  value       = snowflake_warehouse.finnhub.name
}

output "snowflake_schema_names" {
  description = "Snowflake schemas created for the layered data model."
  value       = sort([for schema in snowflake_schema.layers : schema.name])
}