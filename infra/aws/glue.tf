# -----------------------------
# Glue Catalog Database: RAW
# -----------------------------
resource "aws_glue_catalog_database" "raw_db" {
  name = "${var.project}_raw_db"
}

# -----------------------------
# Glue Catalog Database: PROCESSED
# -----------------------------
resource "aws_glue_catalog_database" "processed_db" {
  name = "${var.project}_processed_db"
}

# -----------------------------
# Glue Crawler: RAW Ecommerce Data
# -----------------------------
resource "aws_glue_crawler" "ecommerce" {
  name          = "salesdata-crawler-ecommerce"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.raw_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/raw/ecommerce/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  depends_on = [
    aws_s3_bucket.raw,
    aws_iam_role.glue_role
  ]
}

# -----------------------------
# Glue Crawler: PROCESSED Ecommerce Data (Cleaned CSV)
# -----------------------------
resource "aws_glue_crawler" "processed_ecommerce" {
  name          = "salesdata-crawler-processed-ecommerce"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.processed_db.name

  s3_target {
    path = "s3://${aws_s3_bucket.processed.bucket}/cleaned-csv/ecommerce/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  depends_on = [
    aws_s3_bucket.processed,
    aws_iam_role.glue_role,
    aws_glue_catalog_database.processed_db
  ]
}
