provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "datalake" {
  bucket = var.bucket_name

  force_destroy = false

  tags = {
    Project = "financial-forecast-data-platform"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
