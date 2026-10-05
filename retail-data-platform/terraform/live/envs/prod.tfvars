environment            = "prod"
glue_worker_type       = "G.2X"
glue_number_of_workers = 10
etl_schedule_cron      = "cron(30 1 * * ? *)" # 01:30 UTC = 07:00 IST daily
raw_retention_days     = 2555                 # 7 years
log_retention_days     = 90
max_quarantined_rows   = 1000
alert_emails           = ["data-oncall@example.com"]
kinesis_stream_name    = "retail-transactions-prod"
# secret_arns          = ["arn:aws:secretsmanager:ap-south-1:111122223333:secret:retail/redshift-loader-AbCdEf"]
