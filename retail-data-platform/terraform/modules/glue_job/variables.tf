variable "name_prefix" { type = string }
variable "environment" { type = string }

variable "job_short_name" {
  description = "Short job name, e.g. 'transactions-etl'"
  type        = string
}

variable "script_local_path" {
  description = "Path (in the repo) to the PySpark script to deploy"
  type        = string
}

variable "bucket_name" { type = string }
variable "kms_key_arn" { type = string }
variable "role_arn" { type = string }
variable "glue_database" { type = string }
variable "log_group_name" { type = string }

variable "glue_version" {
  type    = string
  default = "4.0"
}

variable "worker_type" {
  description = "G.1X (4 vCPU/16GB), G.2X (8 vCPU/32GB) ..."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  type    = number
  default = 2
}

variable "timeout_minutes" {
  type    = number
  default = 60
}

variable "max_retries" {
  type    = number
  default = 1
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "schedule_cron" {
  description = "Glue cron (UTC). null = no schedule. e.g. cron(30 1 * * ? *) = 07:00 IST"
  type        = string
  default     = null
}

variable "schedule_enabled" {
  type    = bool
  default = true
}

variable "extra_arguments" {
  type    = map(string)
  default = {}
}

variable "tags" {
  type    = map(string)
  default = {}
}
