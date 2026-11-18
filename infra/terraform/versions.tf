terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # State is kept locally (and gitignored) to keep this free and simple. On a team it would live in
  # a remote backend such as S3, with locking.
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = "commerce-lakehouse-pipeline"
      ManagedBy = "terraform"
    }
  }
}
