resource "aws_glue_job" "transform_raw_to_cleaned" {
  name     = "ecommerce-glue-clean-transform"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw.bucket}/${aws_s3_object.glue_etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language" = "python"
    "--job-name"     = "ecommerce-glue-clean-transform"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  depends_on = [
    aws_s3_object.glue_etl_script
  ]
}

resource "null_resource" "trigger_glue_job" {
  depends_on = [aws_glue_job.transform_raw_to_cleaned]

  provisioner "local-exec" {
    command = <<EOT
      aws glue start-job-run --job-name ecommerce-glue-clean-transform
    EOT
  }
}
