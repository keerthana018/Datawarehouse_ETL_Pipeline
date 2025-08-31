resource "snowflake_storage_integration" "s3_integration" {
  name                      = "MY_S3_INTEGRATION"
  storage_provider          = "S3"
  enabled                   = true
  storage_aws_role_arn      = "arn:aws:iam::442391649618:role/snowflake-s3-role"
  storage_allowed_locations = ["s3://aws-project-4-processed-data/"]
}
