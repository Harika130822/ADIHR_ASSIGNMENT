# =====================================================================================
# Module: monitoring  -  turns logs and metrics into ALERTS a human will actually see
#
#   Glue job FAILED/TIMEOUT  --EventBridge-->  SNS  --> email / Slack (via Chatbot)
#   "ETL run FAILED" in logs --metric filter--> alarm --> SNS
#   DQ_METRICS json line     --metric filter--> RowsQuarantined metric --> alarm if too many
#   Job did not succeed in 26h ("silent failure")         --> alarm
#   Kinesis consumer falling behind (IteratorAge)         --> alarm   (optional)
# =====================================================================================

resource "aws_sns_topic" "alerts" {
  name              = "${var.name_prefix}-data-alerts-${var.environment}"
  kms_master_key_id = "alias/aws/sns" # encrypted at rest
  tags              = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

data "aws_iam_policy_document" "sns" {
  statement {
    sid       = "AllowEventBridgeAndCloudWatch"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com", "cloudwatch.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns.json
}

# ---------------- 1) Glue job state change -> instant alert ----------------
resource "aws_cloudwatch_event_rule" "glue_failed" {
  name        = "${var.name_prefix}-glue-failed-${var.environment}"
  description = "Glue ETL job failed, timed out or was stopped"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = var.glue_job_names
      state   = ["FAILED", "TIMEOUT", "STOPPED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "glue_failed" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
  input_transformer {
    input_paths    = { job = "$.detail.jobName", state = "$.detail.state", run = "$.detail.jobRunId", msg = "$.detail.message" }
    input_template = "\"[${upper(var.environment)}] Glue job <job> is <state>. Run: <run>. Reason: <msg>\""
  }
}

# ---------------- 2) Error lines in the job log ----------------
resource "aws_cloudwatch_log_metric_filter" "etl_failed" {
  name           = "etl-run-failed"
  log_group_name = var.etl_log_group_name
  pattern        = "\"ETL run FAILED\""
  metric_transformation {
    name      = "EtlRunFailed"
    namespace = var.metrics_namespace
    value     = "1"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_metric_alarm" "etl_failed" {
  alarm_name          = "${var.name_prefix}-etl-failed-${var.environment}"
  alarm_description   = "The ETL logged a fatal error. Check the log group ${var.etl_log_group_name}."
  namespace           = var.metrics_namespace
  metric_name         = "EtlRunFailed"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# ---------------- 3) Data quality: too many rows quarantined ----------------
resource "aws_cloudwatch_log_metric_filter" "rows_quarantined" {
  name           = "dq-rows-quarantined"
  log_group_name = var.etl_log_group_name
  pattern        = "{ $.event = \"DQ_METRICS\" }"
  metric_transformation {
    name      = "RowsQuarantined"
    namespace = var.metrics_namespace
    value     = "$.rows_quarantined"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_log_metric_filter" "rows_valid" {
  name           = "dq-rows-valid"
  log_group_name = var.etl_log_group_name
  pattern        = "{ $.event = \"DQ_METRICS\" }"
  metric_transformation {
    name      = "RowsValid"
    namespace = var.metrics_namespace
    value     = "$.rows_valid"
    unit      = "Count"
  }
}

resource "aws_cloudwatch_metric_alarm" "dq_quarantine_high" {
  alarm_name          = "${var.name_prefix}-dq-quarantine-high-${var.environment}"
  alarm_description   = "More than ${var.max_quarantined_rows} rows failed data-quality rules in one run - check the source system."
  namespace           = var.metrics_namespace
  metric_name         = "RowsQuarantined"
  statistic           = "Maximum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = var.max_quarantined_rows
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# ---------------- 4) Silent failure: no successful load in 26 hours ----------------
resource "aws_cloudwatch_metric_alarm" "no_data_loaded" {
  alarm_name          = "${var.name_prefix}-no-data-loaded-${var.environment}"
  alarm_description   = "No ETL run reported valid rows in the last ~26h (job not scheduled, or upstream sent nothing)."
  namespace           = var.metrics_namespace
  metric_name         = "RowsValid"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 26
  datapoints_to_alarm = 26
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching" # missing data IS the problem here
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# ---------------- 5) Streaming lag (optional) ----------------
resource "aws_cloudwatch_metric_alarm" "kinesis_iterator_age" {
  count               = var.kinesis_stream_name == null ? 0 : 1
  alarm_name          = "${var.name_prefix}-stream-consumer-lag-${var.environment}"
  alarm_description   = "Consumers are more than ${var.max_iterator_age_ms / 60000} min behind the Kinesis stream (back-pressure / consumer down)."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  dimensions          = { StreamName = var.kinesis_stream_name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = var.max_iterator_age_ms
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# ---------------- One dashboard for the on-call engineer ----------------
resource "aws_cloudwatch_dashboard" "this" {
  dashboard_name = "${var.name_prefix}-data-platform-${var.environment}"
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric", x = 0, y = 0, width = 12, height = 6
        properties = {
          title   = "Rows per ETL run (valid vs quarantined)"
          region  = var.aws_region
          stat    = "Sum"
          period  = 3600
          metrics = [[var.metrics_namespace, "RowsValid"], [var.metrics_namespace, "RowsQuarantined"]]
        }
      },
      {
        type = "log", x = 12, y = 0, width = 12, height = 6
        properties = {
          title  = "Latest DQ metrics"
          region = var.aws_region
          query  = "SOURCE '${var.etl_log_group_name}' | filter event = 'DQ_METRICS' | fields @timestamp, rows_read, rows_valid, rows_quarantined, duplicates_removed, duration_sec | sort @timestamp desc | limit 20"
        }
      }
    ]
  })
}
