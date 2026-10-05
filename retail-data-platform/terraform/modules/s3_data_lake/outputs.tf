output "bucket_name" {
  value = aws_s3_bucket.lake.id
}

output "bucket_arn" {
  value = aws_s3_bucket.lake.arn
}

output "kms_key_arn" {
  value = aws_kms_key.lake.arn
}

output "layer_uris" {
  description = "S3 URIs of each data-lake layer"
  value = {
    raw        = "s3://${aws_s3_bucket.lake.id}/raw"
    processed  = "s3://${aws_s3_bucket.lake.id}/processed"
    curated    = "s3://${aws_s3_bucket.lake.id}/curated"
    quarantine = "s3://${aws_s3_bucket.lake.id}/quarantine"
    scripts    = "s3://${aws_s3_bucket.lake.id}/scripts"
    state      = "s3://${aws_s3_bucket.lake.id}/_state"
  }
}
