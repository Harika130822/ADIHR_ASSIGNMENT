# Remote state for prod. Bucket is created once by a bootstrap step (versioned + encrypted).
bucket       = "retail-terraform-state-<account-id>"
key          = "retail-data-platform/prod/terraform.tfstate"
region       = "ap-south-1"
encrypt      = true
use_lockfile = true   # S3-native state locking (Terraform >= 1.10); use dynamodb_table on older versions
