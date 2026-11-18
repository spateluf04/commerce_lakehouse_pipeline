data "aws_caller_identity" "current" {}

locals {
  bucket_name = "${var.bucket_prefix}-${data.aws_caller_identity.current.account_id}"
}

# --- Bucket ------------------------------------------------------------------

resource "aws_s3_bucket" "lakehouse" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ACLs off: access is controlled only by IAM and the bucket policy.
resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# SSE-S3 encryption at rest: free, and nothing to manage.
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Delta overwrites and VACUUM leave old object versions behind once versioning is on. Expire them.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_retention_days
    }
    expiration {
      expired_object_delete_marker = true
    }
  }

  # Failed multipart uploads are invisible but still billed.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }

  rule {
    id     = "expire-landing-files"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    expiration {
      days = var.landing_retention_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.lakehouse]
}

# Refuse any request that doesn't use TLS.
resource "aws_s3_bucket_policy" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = data.aws_iam_policy_document.tls_only.json

  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
}

data "aws_iam_policy_document" "tls_only" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.lakehouse.arn, "${aws_s3_bucket.lakehouse.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

# --- Least-privilege identity for the pipeline -------------------------------

resource "aws_iam_user" "pipeline" {
  name = "${var.bucket_prefix}-pipeline"
}

# Only what Spark's S3A connector, Delta Lake and the landing upload need, on this bucket only.
# No s3:* and no other buckets. Access keys are deliberately NOT created here, because their secret
# would end up in the Terraform state file. See the README for creating them with the AWS CLI.
data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListTheBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
    resources = [aws_s3_bucket.lakehouse.arn]
  }

  statement {
    sid = "ReadWriteObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject", # S3A deletes directory markers and temp files; Delta VACUUM deletes old files
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.lakehouse.arn}/*"]
  }
}

resource "aws_iam_policy" "pipeline" {
  name        = "${var.bucket_prefix}-pipeline-s3"
  description = "Read/write access to the commerce lakehouse bucket only"
  policy      = data.aws_iam_policy_document.pipeline.json
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}

# --- Optional cost guardrail -------------------------------------------------

resource "aws_budgets_budget" "monthly" {
  count        = var.budget_alert_email == null ? 0 : 1
  name         = "${var.bucket_prefix}-monthly"
  budget_type  = "COST"
  limit_amount = "1"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}
