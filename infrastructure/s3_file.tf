resource "aws_s3_object" "glue_job_code" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "glue-job-code/etl-csv-to-parquet.py"
  acl    = "private"
  source = "../scripts/ETL-CSV-TO-PARQUET.py"

  etag = filemd5("../scripts/ETL-CSV-TO-PARQUET.py")
}

resource "aws_s3_object" "rais_vinc_pub_centro_oeste" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "raw-data/rais/RAIS_VINC_PUB_CENTRO_OESTE/RAIS_VINC_PUB_CENTRO_OESTE.txt"
  acl    = "private"
  source = "../data/RAIS_VINC_PUB_CENTRO_OESTE.txt"

  etag = filemd5("../data/RAIS_VINC_PUB_CENTRO_OESTE.txt")
}

resource "aws_s3_object" "rais_vinc_pub_mg_es_rj" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "raw-data/rais/RAIS_VINC_PUB_MG_ES_RJ/RAIS_VINC_PUB_MG_ES_RJ.txt"
  acl    = "private"
  source = "../data/RAIS_VINC_PUB_MG_ES_RJ.txt"

  etag = filemd5("../data/RAIS_VINC_PUB_MG_ES_RJ.txt")
}

resource "aws_s3_object" "rais_vinc_pub_nordeste" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "raw-data/rais/RAIS_VINC_PUB_NORDESTE/RAIS_VINC_PUB_NORDESTE.txt"
  acl    = "private"
  source = "../data/RAIS_VINC_PUB_NORDESTE.txt"

  etag = filemd5("../data/RAIS_VINC_PUB_NORDESTE.txt")
}

resource "aws_s3_object" "rais_vinc_pub_norte" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "raw-data/rais/RAIS_VINC_PUB_NORTE/RAIS_VINC_PUB_NORTE.txt"
  acl    = "private"
  source = "../data/RAIS_VINC_PUB_NORTE.txt"

  etag = filemd5("../data/RAIS_VINC_PUB_NORTE.txt")
}

resource "aws_s3_object" "rais_vinc_pub_sp" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "raw-data/rais/RAIS_VINC_PUB_SP/RAIS_VINC_PUB_SP.txt"
  acl    = "private"
  source = "../data/RAIS_VINC_PUB_SP.txt"

  etag = filemd5("../data/RAIS_VINC_PUB_SP.txt")
}

resource "aws_s3_object" "rais_vinc_pub_sul" {
  bucket = aws_s3_bucket.datalake_bucket.id
  key    = "raw-data/rais/RAIS_VINC_PUB_SUL/RAIS_VINC_PUB_SUL.txt"
  acl    = "private"
  source = "../data/RAIS_VINC_PUB_SUL.txt"

  etag = filemd5("../data/RAIS_VINC_PUB_SUL.txt")
}