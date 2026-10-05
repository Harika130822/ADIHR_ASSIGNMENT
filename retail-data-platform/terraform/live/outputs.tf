output "data_lake_bucket" {
  value = module.data_lake.bucket_name
}

output "data_lake_layers" {
  value = module.data_lake.layer_uris
}

output "glue_job_name" {
  value = module.transactions_etl.job_name
}

output "glue_role_arn" {
  value = module.glue_role.role_arn
}

output "alerts_topic_arn" {
  value = module.monitoring.alerts_topic_arn
}
