variable "aws_region" {
  description = "AWS region where the raw Finnhub S3 bucket will be created."
  type        = string
  default     = "us-east-1"
}

variable "raw_bucket_name" {
  description = "Globally unique S3 bucket name for raw Finnhub stock quote JSON/JSONL files."
  type        = string
}

variable "snowflake_organization_name" {
  description = "Snowflake organization name, often shown in Snowsight account details."
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name/identifier within the organization."
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username Terraform will use."
  type        = string
}

variable "snowflake_password" {
  description = "Optional Snowflake password. Prefer setting with TF_VAR_snowflake_password instead of terraform.tfvars."
  type        = string
  sensitive   = true
  default     = null
}

variable "snowflake_private_key_file" {
  description = "Optional local path to a Snowflake private key file for key-pair authentication."
  type        = string
  sensitive   = true
  default     = null
}

variable "snowflake_role" {
  description = "Snowflake role with privileges to create databases, warehouses, and schemas."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_database_name" {
  description = "Name of the Snowflake database for this Finnhub project."
  type        = string
  default     = "FINNHUB_STOCKS_MDS"
}

variable "snowflake_warehouse_name" {
  description = "Name of the Snowflake virtual warehouse used by the project."
  type        = string
  default     = "FINNHUB_STOCKS_MDS_WH"
}

variable "snowflake_warehouse_size" {
  description = "Snowflake warehouse size. XSMALL is a safe beginner default for cost control."
  type        = string
  default     = "XSMALL"
}

variable "snowflake_auto_suspend_seconds" {
  description = "Seconds of inactivity before the warehouse auto-suspends to help reduce cost."
  type        = number
  default     = 60
}

variable "snowflake_schemas" {
  description = "Layered schemas used by the RAW, STAGING, INTERMEDIATE, and MARTS data model."
  type        = set(string)
  default     = ["RAW", "STAGING", "INTERMEDIATE", "MARTS"]
}