resource "aws_s3_bucket" "datalake_bucket" {
  bucket = "${var.bucket_name_base}-${var.account_id}-tf"
  acl = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake_bucket_encryption" {
  bucket = aws_s3_bucket.datalake_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "AES256"
    }
  }
}