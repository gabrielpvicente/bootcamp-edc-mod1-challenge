resource "aws_glue_catalog_database" "aws_glue_catalog_database" {
  name = "rais_db"
}

resource "aws_glue_crawler" "glue_crawler" {
  database_name = aws_glue_catalog_database.aws_glue_catalog_database.id
  name          = "bootcamp-edc-mod1-challenge-rais"
  role          = aws_iam_role.glue_role.arn
  table_prefix  = "parquet_tbl_"

  s3_target {
    path = "s3://${aws_s3_bucket.datalake_bucket.id}/spec/rais/"
  }

}