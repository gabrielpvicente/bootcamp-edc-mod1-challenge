resource "aws_cloudwatch_log_group" "etl_glue_job_log" {
  name              = "example"
  retention_in_days = 2
}

resource "aws_glue_job" "etl_glue_job" {
  name     = "ETL-CSV-TO-PARQUET-TF"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  max_retries = 0

  command {
    script_location = "${aws_s3_bucket.aws_s3_bucket.datalake_bucket.id}glue-job-code/etl-csv-to-parquet.py"
  }

  default_arguments = {
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.etl_glue_job_log.name
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-auto-scaling "             = "true"
  }
}