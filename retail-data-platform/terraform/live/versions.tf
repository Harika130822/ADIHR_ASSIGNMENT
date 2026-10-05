terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
  # Remote state: one state file per environment, chosen at init time:
  #   terraform init -backend-config=backend/dev.hcl
  backend "s3" {}
}

provider "aws" {
  region = var.aws_region
  # No credentials here. Jenkins assumes a per-environment deploy role (OIDC / instance profile).
  default_tags {
    tags = {
      Project     = var.name_prefix
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = var.owner
      DataClass   = "confidential"
    }
  }
}
