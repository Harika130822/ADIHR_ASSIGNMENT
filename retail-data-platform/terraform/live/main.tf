# =====================================================================================
# Root module: composes the reusable modules into one environment.
# The SAME code deploys dev and prod; only envs/<env>.tfvars and backend/<env>.hcl differ.
# =====================================================================================

data "aws_caller_identity" "current" {}

locals {
  account_suffix = substr(data.aws_caller_identity.current.account_id, -6, 6)
  glue_database  = "${var.name_prefix}_lake_${var.environment}"
  etl_log_group  = "/aws-glue/${var.name_prefix}/${var.environment}/transactions-etl"
}

module "data_lake" {
  source                    = "../modules/s3_data_lake"
  name_prefix               = var.name_prefix
  environment               = var.environment
  account_suffix            = local.account_suffix
  force_destroy             = var.environment != "prod"
  raw_retention_days        = var.raw_retention_days
  quarantine_retention_days = var.quarantine_retention_days
}

module "glue_role" {
  source         = "../modules/iam_glue_role"
  name_prefix    = var.name_prefix
  environment    = var.environment
  bucket_arn     = module.data_lake.bucket_arn
  kms_key_arn    = module.data_lake.kms_key_arn
  glue_database  = local.glue_database
  log_group_name = local.etl_log_group
  secret_arns    = var.secret_arns
}

module "transactions_etl" {
  source             = "../modules/glue_job"
  name_prefix        = var.name_prefix
  environment        = var.environment
  job_short_name     = "transactions-etl"
  script_local_path  = "${path.root}/../../pyspark/transactions_etl.py"
  bucket_name        = module.data_lake.bucket_name
  kms_key_arn        = module.data_lake.kms_key_arn
  role_arn           = module.glue_role.role_arn
  glue_database      = local.glue_database
  log_group_name     = local.etl_log_group
  worker_type        = var.glue_worker_type
  number_of_workers  = var.glue_number_of_workers
  schedule_cron      = var.etl_schedule_cron
  log_retention_days = var.log_retention_days
}

module "monitoring" {
  source               = "../modules/monitoring"
  name_prefix          = var.name_prefix
  environment          = var.environment
  aws_region           = var.aws_region
  alert_emails         = var.alert_emails
  glue_job_names       = [module.transactions_etl.job_name]
  etl_log_group_name   = module.transactions_etl.log_group_name
  max_quarantined_rows = var.max_quarantined_rows
  kinesis_stream_name  = var.kinesis_stream_name
}
