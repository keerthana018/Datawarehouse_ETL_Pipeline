output "s3_raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}

output "s3_processed_bucket" {
  value = aws_s3_bucket.processed.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}
