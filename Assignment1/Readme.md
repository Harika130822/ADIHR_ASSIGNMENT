
# 1. Solution Architecture

> https://github.com/Harika130822/ADIHR_ASSIGNMENT/blob/main/Assignment1/Architecture.drawio

#### Failure Points and Retry Strategy

| Failure Point | What Can Go Wrong | Detection | Recovery / Retry |
|---------------|-------------------|------------|------------------|
| Source file upload | Late, missing, partial file | "No data in 26h" alarm | Upload is atomic (S3 PUT); job waits for next run, watermark guarantees it's picked up |
| Bad records | Nulls, bad dates, negative amounts | DQ metrics + quarantine alarm | Rows go to quarantine with reasons; job fails only if >10% bad |

#### Scalability

- **Storage:** S3 scales without limits; prefix-per-partition spreads request load.

- **Batch compute:** Glue workers are a parameter (2 in dev, 10 × G.2X in prod); Glue Auto Scaling can add workers per stage. Partition pruning means a daily run reads one day, not the whole history.

- **Streaming:** Kinesis on-demand mode auto-scales shards (or add shards: 1 MB/s or 1,000 records/s each). Spark parallelism follows shard/partition count; `maxOffsetsPerTrigger` caps each micro-batch (back-pressure).

- **Serving:** Athena is serverless; Redshift Serverless scales RPUs; ECS service auto-scales on CPU or request count.

#### Monitoring

CloudWatch Logs (continuous Glue logging) → metric filters turn the job's `DQ_METRICS` JSON line into metrics → alarms → SNS email/Slack.

One CloudWatch dashboard shows rows valid vs quarantined per run.

All defined in Terraform (`terraform/modules/monitoring`).

#### Security Boundaries

1. **Network:** Glue/EMR/ECS run in private subnets; S3, KMS, Glue and Kinesis are reached through VPC endpoints, not the internet. Only the ALB is public.

2. **Identity:** Each component has its own IAM role scoped to the prefixes it needs (Glue reads `raw/`, writes `processed/`).

3. **Data:** Everything encrypted with a customer-managed KMS key; TLS enforced by bucket policy.

4. **Accounts:** Dev and prod in separate AWS accounts; CI/CD assumes a deploy role per account.


# 2. Data Ingestion and Data Lake Design

```
s3://retail-data-lake-prod-<acct>/
├── raw/transactions/ingest_date=2026-09-01/transactions_20260901.csv     # as received, immutable
├── processed/transactions/year=2026/month=09/day=01/part-00000-<uuid>.snappy.parquet
├── curated/
│   ├── daily_region_sales/year=2026/month=09/part-....snappy.parquet
│   └── daily_sales/year=2026/month=09/part-....snappy.parquet
├── quarantine/transactions/ingest_date=2026-09-01/...     # rows that failed DQ, with reasons
├── dlq/transactions_stream/ingest_date=.../               # malformed streaming events
├── scripts/transactions-etl/<md5>/transactions_etl.py      # versioned job code
└── _state/transactions-etl/watermark.json                  # incremental bookmark
```

#### Raw Data Partitioning

| Decision | Choice | Justification |
|----------|----------|----------|
| Raw partition key | `ingest_date` (when data arrived) | Raw data is organized by arrival, so incremental jobs can pick up only what is new, even when a file contains old transactions |
| Processed/curated partition key | `year/month/day` of `transaction_ts` | Analysts filter by business date; Athena/Spark skip every other folder/partition during queries |


#### Lifecycle Policy

| Prefix | Rule | Why |
|----------|----------|----------|
| `raw/` | Standard → Standard-IA at 30 days → Glacier Instant Retrieval at 90 days → Delete at 7 years | Rarely re-read after a month; kept for audit/replay |
| `processed/` | Intelligent-Tiering from day 0 | Access pattern unknown; S3 moves cold objects automatically |
| `curated/` | Standard, no expiry | Hot, small, used by dashboards |


#### Small-file management
Small files (thousands of 50 KB files) slow Spark and Athena because each file costs a request and task overhead. Target 128 MB–1 GB per file.
1. At write time: the ETL calls repartition("year","month","day") before writing, giving one file per daily partition (verified: 120 days → 120 files).
2. Partition granularity: aggregates partitioned by month, not day.
3. Streaming: micro-batches produce many small files — a nightly compaction job rewrites yesterday's stream partition into a few large files (or use Apache Iceberg tables with rewrite_data_files).
4. Ingestion: Firehose buffering (e.g. 128 MB or 300 s) before writing to S3.
5. Monitor: S3 Storage Lens / an Athena query on "$path" to count files per partition.
   
> <img width="1114" height="604" alt="image" src="https://github.com/user-attachments/assets/558f77ae-f4cd-4f6f-850b-be639dc357bd" />



# 3. ETL and Pyspark Implementation

pyspark/transactions_etl.py is one script that runs both on your laptop (--local) and on AWS Glue unchanged. On the sample data it read 34,320 rows, quarantined 832 bad ones, removed 1,286 duplicates, and a rerun with no new data correctly did nothing.

#### Requirement Mapping to Implementation

| Assignment Requirement | Function | How It Works |
|------------------------|----------|--------------|
| Year/Month/Day partitioning | `add_date_parts()` | Derived from the parsed timestamp and used as partition columns |
| Region-wise and day-wise aggregations | `aggregate()` | Generates `daily_region_sales` (count, sum, average ticket, unique customers) and `daily_sales` for completed transactions only |
| Parquet + partitioning | `write_parquet()` | Writes data in Snappy-compressed Parquet format, partitioned by year, month, and day, with one file per partition |
| Error handling | `main()` with `try/except` | Logs the stack trace and re-raises exceptions so Glue marks the run as **FAILED**; fails fast if more than 10% of rows are bad (circuit breaker) |
| Logging | `logging` + JSON `DQ_METRICS` record | Sent to CloudWatch; metric filters convert log entries into alarms |
| Incremental processing | `read_watermark()`, `extract()`, `merge_with_existing()` | Processes only records where `ingest_date > last_watermark`; merges late-arriving records into older partitions; dynamic partition overwrite makes reruns idempotent |

#### Requirement to Code Mapping

| Assignment Requirement | Function | How It Works |
|------------------------|----------|--------------|
| Deduplication on business key | `deduplicate()` | Uses a window function partitioned by `transaction_id` ordered by `ingest_ts DESC`; retains `row_number() = 1`, removing duplicates while applying late corrections (`PENDING → COMPLETED`) |
| Null handling | `standardise()` | Trims strings, converts empty strings (`""`) and spaces (`" "`) to `NULL`, defaults currency to `INR`, and `payment_method` to `UNKNOWN` |
| Data quality checks | `apply_dq_rules()` | Executes six validation rules (missing id/customer/amount, invalid date, amount ≤ 0, unknown region). Invalid rows are sent to quarantine along with the list of violated rules |
| Date normalization | `parse_ts()` | Attempts parsing using three source formats with `coalesce(to_timestamp(...))`; impossible dates such as `31/02` become `NULL` and are quarantined |


# 4. Data Modelling

A star schema with one fact table at the grain of one row per sales transaction, joined to three dimensions by surrogate keys. DDL is in sql/01_star_schema_ddl.sql, the load in sql/02_load_star_schema.sql; the test runner built it from the ETL output with 35,134 fact rows, 800 customers, 5 regions and zero orphan keys.


### Star Schema

The data model follows a **star schema** with one fact table and three dimension tables.

#### Schema Overview

- **Fact Table:** `fact_transactions`
- **Dimension Tables:**
  - `dim_customer`
  - `dim_date`
  - `dim_region`

Each dimension row can relate to many fact rows, while each fact row references exactly one customer, one date, and one region.

**Grain:** One fact row represents one sales transaction. This ensures that measures such as amount, quantity, and revenue aggregate correctly across all dimensions.

#### Entity Relationship

```text
                  Dim_Date
                (date_key)
                      |
                      | 1:N
                      |
        +-----------------------------+
        |      Fact_Transactions       |
        |-----------------------------|
        | transaction_id (PK)         |
        | date_key (FK)               |
        | customer_key (FK)           |
        | region_key (FK)             |
        | amount                      |
        | quantity                    |
        | unit_price                  |
        | status                      |
        | payment_method              |
        | store_id                    |
        +-----------------------------+
             /                    \
           1:N                    1:N
           /                        \
          /                          \
 Dim_Customer                  Dim_Region
 (customer_key)               (region_key)

