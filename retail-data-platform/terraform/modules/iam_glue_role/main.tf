# =====================================================================================
# Module: iam_glue_role  -  least-privilege execution role for the Glue ETL job
#
# Principle: the job can READ raw/ and scripts/, WRITE only the layers it produces,
# use only THIS lake's KMS key, write only its own log group, touch only its own
# catalog database, and read only the one secret it needs. Nothing uses "*" on data.
# =====================================================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  account = data.aws_caller_identity.current.account_id
  region  = data.aws_region.current.name
  part    = data.aws_partition.current.partition
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
    # confused-deputy protection: only Glue acting for THIS account can assume the role
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account]
    }
  }
}

resource "aws_iam_role" "glue" {
  name                 = "${var.name_prefix}-glue-etl-${var.environment}"
  assume_role_policy   = data.aws_iam_policy_document.assume.json
  max_session_duration = 3600
  permissions_boundary = var.permissions_boundary_arn
  tags                 = var.tags
}

data "aws_iam_policy_document" "glue" {
  statement {
    sid       = "ListLakeBucketOnlyForOurPrefixes"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.bucket_arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = concat([for p in concat(var.read_prefixes, var.write_prefixes) : "${p}*"], [""])
    }
  }

  statement {
    sid       = "ReadInputs"
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [for p in concat(var.read_prefixes, var.write_prefixes) : "${var.bucket_arn}/${p}*"]
  }

  statement {
    sid       = "WriteOutputs"
    actions   = ["s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [for p in var.write_prefixes : "${var.bucket_arn}/${p}*"]
  }

  statement {
    sid       = "UseLakeKmsKeyOnly"
    actions   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }

  statement {
    sid     = "WriteOwnLogsOnly"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:CreateLogGroup", "logs:AssociateKmsKey"]
    resources = [
      "arn:${local.part}:logs:${local.region}:${local.account}:log-group:${var.log_group_name}:*",
      "arn:${local.part}:logs:${local.region}:${local.account}:log-group:/aws-glue/*",
    ]
  }

  statement {
    sid       = "PublishMetricsToOwnNamespace"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"] # PutMetricData has no resource-level permissions...
    condition {       # ...so we restrict it by namespace instead
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["Glue", var.metrics_namespace]
    }
  }

  statement {
    sid = "GlueCatalogForOwnDatabase"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:GetPartition", "glue:GetPartitions", "glue:BatchCreatePartition",
    "glue:BatchGetPartition", "glue:UpdatePartition"]
    resources = [
      "arn:${local.part}:glue:${local.region}:${local.account}:catalog",
      "arn:${local.part}:glue:${local.region}:${local.account}:database/${var.glue_database}",
      "arn:${local.part}:glue:${local.region}:${local.account}:table/${var.glue_database}/*",
    ]
  }

  dynamic "statement" {
    for_each = length(var.secret_arns) > 0 ? [1] : []
    content {
      sid       = "ReadOnlyTheSecretsThisJobNeeds"
      actions   = ["secretsmanager:GetSecretValue"]
      resources = var.secret_arns
    }
  }
}

resource "aws_iam_role_policy" "glue" {
  name   = "least-privilege-etl"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue.json
}
