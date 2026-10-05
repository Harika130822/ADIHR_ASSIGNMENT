environment            = "dev"
glue_worker_type       = "G.1X"
glue_number_of_workers = 2
etl_schedule_cron      = null # run on demand in dev
raw_retention_days     = 30   # dev data is synthetic - keep it short
log_retention_days     = 7
max_quarantined_rows   = 5000
alert_emails           = [] # e.g. ["you@example.com"]
