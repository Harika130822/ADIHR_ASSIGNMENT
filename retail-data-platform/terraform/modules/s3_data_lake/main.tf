# =====================================================================================
# Module: s3_data_lake
# One encrypted, versioned bucket per environment with raw / processed / curated layers
# (plus quarantine, dlq, scripts, _state). Encryption, TLS-only access, public-access
# block, access logging and per-layer lifecycle rules are all enforced here, so every
# environment gets the same guardrails.
# =====================================================================================

locals {
  bucket_name = "${var.name_prefix}-data-lake-${var.environment}-${var.account_suffix}"
  tags        = merge(var.tags, { Module = "s3_data_lake", Environment = var.environment })
}

# Customer-managed KMS key: lets us control WHO can decrypt, and audit every use in CloudTrail
resource "aws_kms_key" "lake" {
  description             = "Encrypts the ${var.environment} retail data lake"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  tags                    = local.tags
}

resource "aws_kms_alias" "lake" {
  name          = "alias/${var.name_prefix}-data-lake-${var.environment}"
  target_key_id = aws_kms_key.lake.key_id
}

# ---------------- access-log bucket (audit trail of every object request) ----------------
resource "aws_s3_bucket" "logs" {
  bucket        = "${local.bucket_name}-access-logs"
  force_destroy = var.force_destroy
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" } # S3 log delivery requires SSE-S3
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "expire-access-logs"
    status = "Enabled"
    filter {}
    expiration { days = var.access_log_retention_days }
  }
}

# ---------------- the data lake bucket ----------------
resource "aws_s3_bucket" "lake" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy # false in prod: terraform destroy can never wipe data
  tags          = local.tags
}

resource "aws_s3_bucket_ownership_controls" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule { object_ownership = "BucketOwnerEnforced" } # ACLs disabled; IAM policies only
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" } # protects against accidental overwrite/delete
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.lake.arn
    }
    bucket_key_enabled = true # cuts KMS request cost by ~99% for Spark workloads
  }
}

resource "aws_s3_bucket_logging" "lake" {
  bucket        = aws_s3_bucket.lake.id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access/"
}

# Encryption IN TRANSIT: reject any request that is not HTTPS
data "aws_iam_policy_document" "lake" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lake" {
  bucket     = aws_s3_bucket.lake.id
  policy     = data.aws_iam_policy_document.lake.json
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

# Create the layer "folders" so they show up in the console from day one
resource "aws_s3_object" "layers" {
  for_each = toset(["raw/", "processed/", "curated/", "quarantine/", "dlq/", "scripts/", "_state/"])
  bucket   = aws_s3_bucket.lake.id
  key      = each.value
  content  = ""
}

# ---------------- lifecycle: each layer has a different value over time ----------------
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket     = aws_s3_bucket.lake.id
  depends_on = [aws_s3_bucket_versioning.lake]

  # raw = immutable source of truth, rarely read after a few weeks -> tier down, keep for compliance
  rule {
    id     = "raw-tiering"
    status = "Enabled"
    filter { prefix = "raw/" }
    transition {
      days          = var.raw_ia_after_days
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = var.raw_glacier_after_days
      storage_class = "GLACIER_IR"
    }
    expiration { days = var.raw_retention_days }
  }

  # processed = rebuildable from raw, read by ad-hoc analysis -> let S3 pick the tier
  rule {
    id     = "processed-intelligent-tiering"
    status = "Enabled"
    filter { prefix = "processed/" }
    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # quarantine / dlq = only needed long enough to investigate and replay
  rule {
    id     = "quarantine-expiry"
    status = "Enabled"
    filter { prefix = "quarantine/" }
    expiration { days = var.quarantine_retention_days }
  }
  rule {
    id     = "dlq-expiry"
    status = "Enabled"
    filter { prefix = "dlq/" }
    expiration { days = var.quarantine_retention_days }
  }

  # housekeeping for the whole bucket
  rule {
    id     = "housekeeping"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
    noncurrent_version_expiration { noncurrent_days = var.noncurrent_version_days }
  }
}
