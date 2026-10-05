variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "name_prefix" {
  type    = string
  default = "retail"
}

variable "environment" {
  type = string
}

variable "owner" {
  type    = string
  default = "data-engineering"
}

variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_number_of_workers" {
  type    = number
  default = 2
}

variable "etl_schedule_cron" {
  type    = string
  default = null
}

variable "raw_retention_days" {
  type    = number
  default = 2555
}

variable "quarantine_retention_days" {
  type    = number
  default = 90
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "alert_emails" {
  type    = list(string)
  default = []
}

variable "max_quarantined_rows" {
  type    = number
  default = 1000
}

variable "kinesis_stream_name" {
  type    = string
  default = null
}

variable "secret_arns" {
  description = "Secrets Manager ARNs (never secret VALUES) the ETL may read"
  type        = list(string)
  default     = []
}
