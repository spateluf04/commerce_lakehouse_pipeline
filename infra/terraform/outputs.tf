output "bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "storage_root" {
  description = "Value for LAKEHOUSE_STORAGE_ROOT in .env"
  value       = "s3a://${aws_s3_bucket.lakehouse.bucket}/lakehouse"
}

output "landing_root" {
  description = "Value for LAKEHOUSE_LANDING_ROOT in .env"
  value       = "s3a://${aws_s3_bucket.lakehouse.bucket}/landing"
}

output "pipeline_user" {
  value = aws_iam_user.pipeline.name
}

output "create_access_key" {
  description = "Run this once, then put the key in an AWS profile (never in git)"
  value       = "aws iam create-access-key --user-name ${aws_iam_user.pipeline.name}"
}
