variable "name_prefix" { type = string }
variable "environment" { type = string }

variable "bucket_arn" {
  description = "ARN of the data-lake bucket"
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the lake KMS key - the ONLY key this role can use"
  type        = string
}

variable "read_prefixes" {
  description = "Prefixes the job may only read"
  type        = list(string)
  default     = ["raw/", "scripts/"]
}

variable "write_prefixes" {
  description = "Prefixes the job may write (and therefore also read)"
  type        = list(string)
  default     = ["processed/", "curated/", "quarantine/", "_state/", "tmp/"]
}

variable "glue_database" {
  type = string
}

variable "log_group_name" {
  type = string
}

variable "metrics_namespace" {
  type    = string
  default = "RetailDataPlatform"
}

variable "secret_arns" {
  description = "Secrets Manager ARNs the job may read (e.g. Redshift credentials). Empty = none."
  type        = list(string)
  default     = []
}

variable "permissions_boundary_arn" {
  description = "Optional org-wide permissions boundary"
  type        = string
  default     = null
}

variable "tags" {
  type    = map(string)
  default = {}
}
