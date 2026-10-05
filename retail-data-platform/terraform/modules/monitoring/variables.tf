variable "name_prefix" { type = string }
variable "environment" { type = string }
variable "aws_region" { type = string }

variable "alert_emails" {
  description = "Who gets paged. Each address must confirm the SNS subscription email."
  type        = list(string)
  default     = []
}

variable "glue_job_names" {
  type = list(string)
}

variable "etl_log_group_name" {
  type = string
}

variable "metrics_namespace" {
  type    = string
  default = "RetailDataPlatform"
}

variable "max_quarantined_rows" {
  type    = number
  default = 1000
}

variable "kinesis_stream_name" {
  type    = string
  default = null
}

variable "max_iterator_age_ms" {
  type    = number
  default = 300000 # 5 minutes
}

variable "tags" {
  type    = map(string)
  default = {}
}
