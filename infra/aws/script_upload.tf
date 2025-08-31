resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.raw.bucket
  key    = "scripts/transform_raw_to_cleaned.py"
  source = "${path.module}/transform_raw_to_cleaned.py"
  etag   = filemd5("${path.module}/transform_raw_to_cleaned.py")
  content_type = "text/x-python"
}
