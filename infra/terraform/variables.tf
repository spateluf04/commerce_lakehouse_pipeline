variable "aws_region" {
  description = "Region for the bucket."
  type        = string
  default     = "us-east-1"
}

variable "bucket_prefix" {
  description = "The bucket is named <bucket_prefix>-<account id>, which keeps it globally unique."
  type        = string
  default     = "commerce-lakehouse"
}

variable "force_destroy" {
  description = "Let `terraform destroy` delete the bucket even when it still holds objects. Fine for a demo; never for real data."
  type        = bool
  default     = true
}

variable "landing_retention_days" {
  description = "Delete raw landing files after this many days (bronze keeps a copy in Delta)."
  type        = number
  default     = 30
}

variable "noncurrent_version_retention_days" {
  description = "Delete overwritten or deleted object versions after this many days, so versioning can't grow storage forever."
  type        = number
  default     = 7
}

variable "budget_alert_email" {
  description = "If set, create a $1/month AWS Budget that emails this address when spend is forecast to go over it."
  type        = string
  default     = null
}
