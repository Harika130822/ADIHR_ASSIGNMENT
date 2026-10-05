variable "name_prefix" {
  description = "Short project prefix used in every resource name, e.g. 'retail'"
  type        = string
}

variable "environment" {
  description = "dev | staging | prod"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging or prod."
  }
}

variable "account_suffix" {
  description = "Makes the globally-unique bucket name unique (e.g. last 6 digits of the account id)"
  type        = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket. NEVER true in prod."
  type        = bool
  default     = false
}

variable "raw_ia_after_days" {
  type    = number
  default = 30
}

variable "raw_glacier_after_days" {
  type    = number
  default = 90
}

variable "raw_retention_days" {
  description = "How long raw source data is kept (e.g. 7 years = 2555 days for financial records)"
  type        = number
  default     = 2555
}

variable "quarantine_retention_days" {
  type    = number
  default = 90
}

variable "noncurrent_version_days" {
  type    = number
  default = 30
}

variable "access_log_retention_days" {
  type    = number
  default = 365
}

variable "tags" {
  type    = map(string)
  default = {}
}
