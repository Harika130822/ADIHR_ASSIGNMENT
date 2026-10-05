# =====================================================================================
# Module: glue_job  -  Glue Spark ETL job + its catalog database, logs, schedule, crawler
# =====================================================================================

locals {
  job_name   = "${var.name_prefix}-${var.job_short_name}-${var.environment}"
  script_key = "scripts/${var.job_short_name}/${filemd5(var.script_local_path)}/${basename(var.script_local_path)}"
  lake       = "s3://${var.bucket_name}"
}

# ---------------- Data Catalog (metadata for Athena / Redshift Spectrum / Lake Formation) ----------------
resource "aws_glue_catalog_database" "this" {
  name        = var.glue_database
  description = "Retail data lake - ${var.environment}"
}

# ---------------- Logs ----------------
resource "aws_cloudwatch_log_group" "job" {
  name              = var.log_group_name
  retention_in_days = var.log_retention_days # never keep logs forever by accident
  tags              = var.tags
}

# ---------------- Encryption for everything Glue writes ----------------
resource "aws_glue_security_configuration" "this" {
  name = "${local.job_name}-sec"
  encryption_configuration {
    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = var.kms_key_arn
    }
    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = var.kms_key_arn
    }
    cloudwatch_encryption {
      # Enable with a KMS key whose key policy allows logs.<region>.amazonaws.com
      cloudwatch_encryption_mode = "DISABLED"
    }
  }
}

# ---------------- Script: content hash in the key = every change is a new, traceable version ----------------
resource "aws_s3_object" "script" {
  bucket = var.bucket_name
  key    = local.script_key
  source = var.script_local_path
  etag   = filemd5(var.script_local_path)
}

# ---------------- The job ----------------
resource "aws_glue_job" "this" {
  name                   = local.job_name
  description            = "Raw -> processed -> curated transactions ETL (${var.environment})"
  role_arn               = var.role_arn
  glue_version           = var.glue_version
  worker_type            = var.worker_type
  number_of_workers      = var.number_of_workers
  timeout                = var.timeout_minutes
  max_retries            = var.max_retries # automatic retry for transient failures
  security_configuration = aws_glue_security_configuration.this.name
  execution_class        = var.environment == "prod" ? "STANDARD" : "FLEX" # FLEX = cheaper for dev

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/${aws_s3_object.script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 1 # two runs at once could race on the watermark
  }

  default_arguments = merge({
    # ---- job parameters (no paths hard-coded in the script) ----
    "--source_path"     = "${local.lake}/raw/transactions"
    "--processed_path"  = "${local.lake}/processed/transactions"
    "--curated_path"    = "${local.lake}/curated"
    "--quarantine_path" = "${local.lake}/quarantine/transactions"
    "--state_path"      = "${local.lake}/_state/${var.job_short_name}"
    "--TempDir"         = "${local.lake}/tmp/${var.job_short_name}/"
    # ---- logging & monitoring ----
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.job.name
    "--enable-continuous-log-filter"     = "true"
    "--enable-metrics"                   = "true"
    "--enable-observability-metrics"     = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "${local.lake}/tmp/spark-ui/${var.job_short_name}/"
    # ---- we use an explicit watermark; bookmarks could be enabled instead ----
    "--job-bookmark-option" = "job-bookmark-disable"
    "--job-language"        = "python"
  }, var.extra_arguments)

  tags = var.tags
}

# ---------------- Schedule (daily) ----------------
resource "aws_glue_trigger" "daily" {
  count    = var.schedule_cron == null ? 0 : 1
  name     = "${local.job_name}-daily"
  type     = "SCHEDULED"
  schedule = var.schedule_cron
  enabled  = var.schedule_enabled
  actions { job_name = aws_glue_job.this.name }
}

# ---------------- Crawler keeps the catalog's partitions in sync after each run ----------------
resource "aws_glue_crawler" "outputs" {
  name          = "${local.job_name}-crawler"
  role          = var.role_arn
  database_name = aws_glue_catalog_database.this.name

  s3_target { path = "${local.lake}/processed/transactions" }
  s3_target { path = "${local.lake}/curated/daily_region_sales" }
  s3_target { path = "${local.lake}/curated/daily_sales" }

  schema_change_policy {
    update_behavior = "LOG" # never silently change a production schema
    delete_behavior = "LOG"
  }
  recrawl_policy { recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY" } # cheap incremental crawls
  configuration = jsonencode({
    Version  = 1.0
    Grouping = { TableGroupingPolicy = "CombineCompatibleSchemas" }
  })
  tags = var.tags
}

resource "aws_glue_trigger" "crawl_after_etl" {
  name = "${local.job_name}-crawl-on-success"
  type = "CONDITIONAL"
  predicate {
    conditions {
      job_name = aws_glue_job.this.name
      state    = "SUCCEEDED"
    }
  }
  actions { crawler_name = aws_glue_crawler.outputs.name }
}
