# IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "${var.project}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Attach AWS Glue Service Role managed policy
resource "aws_iam_role_policy_attachment" "glue_service_role_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Inline policy for S3 access (raw + processed)
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project}-glue-s3-access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw.arn,
          aws_s3_bucket.processed.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.raw.arn}/*",
          "${aws_s3_bucket.processed.arn}/*"
        ]
      }
    ]
  })
}


resource "aws_iam_policy" "snowflake_s3_policy" {
  name = "SnowflakeS3AccessPolicy"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::aws-project-4-processed-data",
          "arn:aws:s3:::aws-project-4-processed-data/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_snowflake_policy" {
  role       = aws_iam_role.snowflake_s3_role.name
  policy_arn = aws_iam_policy.snowflake_s3_policy.arn
}


#snowfake iam
resource "aws_iam_role" "snowflake_s3_role" {
  name = "snowflake-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          AWS = "arn:aws:iam::609124256565:user/kb761000-s"  # Snowflake principal from DESC INTEGRATION
        },
        Action = "sts:AssumeRole",
        Condition = {
          StringEquals = {
            "sts:ExternalId" = "TO94863_SFCRole=6_crW4OO3LyroZLmHq3p2ZzV+vB44="  # Replace this with real value from DESC INTEGRATION
          }
        }
      }
    ]
  })
}
