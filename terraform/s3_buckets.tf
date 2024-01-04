resource "aws_s3_bucket" "config-bucket" {
  bucket = "${local.app_env}-config"

  tags = {
    Name        = "${local.app_env}-s3-bucket"
    Environment = "${var.environment}"
  }
}

# Resource to avoid error "AccessControlListNotSupported: The bucket does not allow ACLs"
resource "aws_s3_bucket_ownership_controls" "s3_bucket_acl_ownership" {
  bucket = aws_s3_bucket.config-bucket.id
  rule {
    object_ownership = "ObjectWriter"
  }
}

resource "aws_s3_bucket_acl" "config-acl" {
  bucket = aws_s3_bucket.config-bucket.id
  acl    = "private"
  depends_on = [aws_s3_bucket_ownership_controls.s3_bucket_acl_ownership]
}

resource "aws_s3_bucket_public_access_block" "ep-backend-public-access-block" {
  bucket = aws_s3_bucket.config-bucket.id

  block_public_acls       = false
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
