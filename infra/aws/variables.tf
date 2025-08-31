variable "aws_region" {
  description = "AWS region where resources will be created"
  type        = string
}

variable "project" {
  description = "Project name prefix"
  type        = string
  default     = "salesdata"
}

variable "snowflake_username" {
  description = "Snowflake username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_role" {
  description = "Snowflake role"
  type        = string
}
