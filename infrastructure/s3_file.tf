resource "aws_s3_object" "glue_job_code" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "glue-job-code/etl-csv-to-parquet.py"
  acl    = "private"
  source = "../scripts/ETL-CSV-TO-PARQUET.py"

  etag = filemd5("../scripts/ETL-CSV-TO-PARQUET.py")
}