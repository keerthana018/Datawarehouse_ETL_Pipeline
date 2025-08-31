# -------------------- RAW BUCKET --------------------
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.project}-raw-data"
  force_destroy = true

  tags = {
    Name        = "${var.project}-raw-data"
    Project     = var.project
    Environment = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"

    # REQUIRED with provider v5: apply to all objects
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ----------------- PROCESSED BUCKET -----------------
resource "aws_s3_bucket" "processed" {
  bucket        = "${var.project}-processed-data"
  force_destroy = true

  tags = {
    Name        = "${var.project}-processed-data"
    Project     = var.project
    Environment = "dev"
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "abort-incomplete-mpu"
    status = "Enabled"

    # REQUIRED with provider v5: apply to all objects
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
