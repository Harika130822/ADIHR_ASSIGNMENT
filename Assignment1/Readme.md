
# 1. Architecture

> https://github.com/Harika130822/ADIHR_ASSIGNMENT/blob/main/Assignment1/Architecture%20(1).drawio

Role of each layer
Layer
AWS services
Role
Sources
POS systems (daily CSV export), e-commerce app (live events), CRM database
Where data is born
Batch ingestion
S3 upload / AWS Transfer Family / AWS DMS for databases
Lands daily files in raw/ exactly as received
Stream ingestion
Kinesis Data Streams (or Amazon MSK for Kafka); Kinesis Firehose as a zero-code archive
Durable, ordered buffer for events; decouples producers from consumers
Storage
S3 bucket with raw/, processed/, curated/ (+ quarantine/, dlq/)
Single source of truth; cheap, durable, versioned
Batch processing
AWS Glue (serverless Spark) — EMR when jobs need custom tuning or run 24×7
Dedup, data-quality checks, normalisation, aggregation
Stream processing
Glue Streaming or EMR running Spark Structured Streaming
Real-time dedup, windowed aggregates, DLQ
Catalog & governance
Glue Data Catalog, Lake Formation, Glue crawlers
Tables, schemas, column-level permissions, lineage
Serving
Athena (ad-hoc SQL), Redshift (star schema for BI), QuickSight dashboards, REST API on ECS Fargate behind an ALB
Delivers data to analysts, dashboards and applications
Security & operations
IAM, KMS, Secrets Manager, CloudTrail, CloudWatch, SNS, EventBridge
Access control, encryption, audit, alerting

End-to-end data flow
Batch: POS uploads transactions_YYYYMMDD.csv to raw/transactions/ingest_date=YYYY-MM-DD/.
A scheduled Glue job (07:00 IST) reads only new partitions (watermark), validates, deduplicates and writes Parquet to processed/, then aggregates into curated/.
On success, a Glue trigger runs a crawler that registers new partitions in the Data Catalog.
Redshift loads the star schema from processed/ (or queries it through Spectrum); QuickSight reads Redshift.
Streaming: the app sends each sale to Kinesis (partition key = customer_id). Spark Structured Streaming consumes it, writes clean events to processed/transactions_stream/ and 5-minute regional sales to curated/region_sales_5min/ within about a minute.
The ECS API reads curated tables via Athena (or a DynamoDB cache for hot lookups) and serves them to apps.
Failure points and retry strategy
Failure point
What can go wrong
Detection
Recovery / retry
Source file upload
Late, missing, partial file
"No data in 26h" alarm
Upload is atomic (S3 PUT); job waits for next run, watermark guarantees it's picked up
Bad records
Nulls, bad dates, negative amounts
DQ metrics + quarantine alarm
Rows go to quarantine/ with reasons; job fails only if >10% bad
Glue job crash
OOM, transient AWS error
EventBridge "Glue Job State Change" → SNS
max_retries = 1; watermark not advanced on failure so a rerun is safe
Rerun / backfill
Double-counting
Uniqueness check (SQL H4)
Dynamic partition overwrite + dedup = idempotent
Kinesis producer
Throttling (ProvisionedThroughputExceeded)
Kinesis metrics
Retry failed records with exponential backoff (in event_producer.py)
Stream consumer
Crash, slow, poison message
IteratorAge alarm, DLQ size
Restart resumes from checkpoint; poison events go to dlq/; 24h–7d stream retention allows replay
Catalog/serving
Schema drift breaks queries
Crawler set to LOG changes, not apply
Schema changes go through code review

Scalability
Storage: S3 scales without limits; prefix-per-partition spreads request load.
Batch compute: Glue workers are a parameter (2 in dev, 10 × G.2X in prod); Glue Auto Scaling can add workers per stage. Partition pruning means a daily run reads one day, not the whole history.
Streaming: Kinesis on-demand mode auto-scales shards (or add shards: 1 MB/s or 1,000 records/s each). Spark parallelism follows shard/partition count; maxOffsetsPerTrigger caps each micro-batch (back-pressure).
Serving: Athena is serverless; Redshift Serverless scales RPUs; ECS service auto-scales on CPU or request count.
Monitoring
CloudWatch Logs (continuous Glue logging) → metric filters turn the job's DQ_METRICS JSON line into metrics → alarms → SNS email/Slack. One CloudWatch dashboard shows rows valid vs quarantined per run. All defined in Terraform (terraform/modules/monitoring).
Security boundaries
Network: Glue/EMR/ECS run in private subnets; S3, KMS, Glue and Kinesis are reached through VPC endpoints, not the internet. Only the ALB is public.
Identity: each component has its own IAM role scoped to the prefixes it needs (Glue reads raw/, writes processed/).
Data: everything encrypted with a customer-managed KMS key; TLS enforced by bucket policy.
Accounts: dev and prod in separate AWS accounts; CI/CD assumes a deploy role per account.
