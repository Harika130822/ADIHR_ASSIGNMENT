output "job_name" {
  value = aws_glue_job.this.name
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.job.name
}

output "glue_database" {
  value = aws_glue_catalog_database.this.name
}

output "script_s3_uri" {
  value = "s3://${var.bucket_name}/${aws_s3_object.script.key}"
}
