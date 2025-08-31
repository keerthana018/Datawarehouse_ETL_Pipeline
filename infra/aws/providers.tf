provider "aws" {
  region = var.aws_region
}

provider "snowflake" {
  username = var.snowflake_username
  # password = var.snowflake_password 
  account = var.snowflake_account
  role    = var.snowflake_role
}
