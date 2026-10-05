# 1. Streaming - Kinesis / kafka / Spark

Producers write each sale to Kinesis Data Streams (or MSK/Kafka) keyed by customer_id; a Spark Structured Streaming job deduplicates within a 2-hour event-time watermark, writes clean events and 5-minute regional totals to S3 with exactly-once file output, and sends malformed events to a dead-letter prefix. The kit's test proves all four behaviours locally.

```
# 2,000 events incl. duplicates, late events and poison messages
python pyspark/streaming/event_producer.py --target files --out samples/stream_in --events 2000
python pyspark/streaming/stream_processor.py --source files --input samples/stream_in --output samples/data/stream --once

# focused test: dedup across batches, late-event drop, DLQ, restart from checkpoint
bash tests/test_streaming_late_and_dupes.sh   # prints PASS
```

# 2. Terraform / Infrastructure as Code

```
terraform/
├── modules/
│   ├── s3_data_lake/    # bucket, KMS key, versioning, TLS-only policy, access logs, lifecycle per layer
│   ├── iam_glue_role/   # least-privilege role: read raw/, write processed/ curated/, this KMS key only
│   ├── glue_job/        # Glue job, catalog DB, log group, security config, schedule, crawler
│   └── monitoring/      # SNS, EventBridge on job failure, metric filters, alarms, dashboard
└── live/                # root module: wires the modules together
    ├── envs/dev.tfvars  envs/prod.tfvars
    └── backend/dev.hcl  backend/prod.hcl
```

#### Requirement Traceability Matrix

| Required Area | Location | Evidence |
|--------------|----------|----------|
| S3 bucket, encryption, and lifecycle management | `s3_data_lake/main.tf` | SSE-KMS with bucket key enabled, customer-managed KMS key with rotation enabled, `DenyInsecureTransport` policy, public access block, versioning enabled, and 5 lifecycle rules configured. |
| IAM least privilege and no hard-coded secrets | `iam_glue_role/main.tf` | Read/write access restricted to specific prefixes, dedicated KMS key permissions, own Glue Catalog database access, `PutMetricData` limited by namespace, `aws:SourceAccount` condition, and secrets passed as ARNs only. |
| Glue job configuration, logging, and parameters | `glue_job/main.tf` | All required paths passed as arguments, continuous CloudWatch logging enabled, metrics and Spark UI enabled, retries configured, timeout configured, `max_concurrent_runs = 1`, security configuration applied, and FLEX execution class used in development. |
| CloudWatch monitoring and alerts | `monitoring/main.tf` | Job FAILED/TIMEOUT notifications sent to SNS, **"ETL run FAILED"** log alarm, quarantine threshold alarm from DQ metrics, **"No Data in 26h"** alarm, Kinesis lag alarm, and operational dashboard. |

```

# one-time: state bucket (replace <acct>)
aws s3api create-bucket --bucket retail-terraform-state-<acct> --region ap-south-1 \
  --create-bucket-configuration LocationConstraint=ap-south-1
aws s3api put-bucket-versioning --bucket retail-terraform-state-<acct> --versioning-configuration Status=Enabled
# edit backend/dev.hcl -> set the bucket name

cd terraform/live
terraform init -backend-config=backend/dev.hcl
terraform fmt -check -recursive .. && terraform validate
terraform plan  -var-file=envs/dev.tfvars -out=dev.tfplan
terraform apply dev.tfplan
terraform output            # bucket name, job name, role ARN

# when finished with screenshots (dev has force_destroy = true)
terraform destroy -var-file=envs/dev.tfvars

```
# 3. CI/CD

# CI/CD Deployment Process

## Deployment Workflow

Nothing reaches production without a reviewed plan and a named approver.

```
```text
Pull Request
      │
      ▼
Validate
(lint, tests, tfsec)
      │
      ▼
Plan Dev
(reviewer reads plan)
      │
      ▼
Merge to Main
      │
      ▼
Apply Dev
(auto on merge)
      │
      ▼
Smoke Test
(Glue job runs)
      │
      ▼
Plan Prod
(saved plan file)
      │
      ▼
Approval
(prod approvers)
      │
      ▼
Apply Prod
(saved plan applied)
```

## How Each Required Control Is Met

| Control | Implementation in the Jenkinsfile |
|----------|----------|
| Validation | Runs `ruff` linting, ETL execution on sample data, SQL tests, `terraform fmt -check`, `terraform validate`, `tflint`, and `tfsec` (fails on HIGH findings) in parallel. |
| Plan / Apply Control | Uses `terraform plan -out=tfplan-<env>`, stashes the generated plan, and applies the exact same plan file. This ensures that what was reviewed is exactly what gets deployed. Terraform also prevents applying stale plans. |
| Environment Separation | Uses separate AWS accounts, a dedicated deployment role per account (`withAWS(role: ...)`), separate Terraform state files, and environment-specific tfvars. No static AWS keys are stored in Jenkins. |
| Approvals | The approval step is restricted to the production approvers group, displays the deployment plan summary, enforces a 4-hour timeout, and records who approved the deployment. |
| Rollback | Every production deployment creates a Git tag. The `ROLLBACK_TO_TAG` parameter redeploys a previous release through the same approval and deployment gates. Data rollback is supported through S3 versioning, and Glue scripts are versioned by hash. |
| Auditability | Plan output and JSON metadata are archived for every build. Approver and commit details are recorded in the tag message. Uses `disableConcurrentBuilds` and Terraform state locking. CloudTrail records every API call made by the deployment role. |


# 4. Advanced SQL

# Analytical Queries (H1 - H5)

| ID | Question | Technique Used | Result on Sample Data |
|----|----------|----------------|----------------------|
| H1 | Top 10 customers by total amount | Uses a CTE with `SUM(amount)` and a `DENSE_RANK()` window function (ties receive the same rank). Only `COMPLETED` sales are included in the revenue calculation. | 10 rows returned |
| H2 | Region-wise sales, last 30 days | Joins to `dim_date`, filters where `full_date >= CURRENT_DATE - 30 days`, and uses `SUM(SUM(amount)) OVER ()` to calculate the overall total in the same query pass. | 5 regions, each contributing 19%–21% of total sales |
| H3 | Month-over-month growth percentage | Monthly totals are calculated in a CTE. `LAG()` is used to retrieve the previous month's value, and `NULLIF(previous_value, 0)` prevents divide-by-zero errors. | Jul +11.2%, Aug -1.58%, Sep +0.61% |
| H4 | Duplicate transactions by business key | Groups by `transaction_id` with `HAVING COUNT(*) > 1` on raw data. The newest version is identified using `ROW_NUMBER()`, showing which record the ETL process retains. | 1,418 duplicate transaction IDs in raw data; 0 duplicates in the fact table |
| H5 | Customers with no transactions in the last 90 days | Uses `NOT EXISTS` (safer with NULL handling than `NOT IN`) and checks all SCD Type 2 versions of a customer to determine the most recent purchase date. | 80 customers identified; matches the 80 dormant customers intentionally generated in the sample data |


# 5. Security & Governance

## Security Controls Implementation

| Area | Implementation Approach | Included in the Solution |
|------|-------------------------|--------------------------|
| Least-Privilege IAM | One IAM role per workload (Glue ETL, Streaming, ECS API, CI/CD deployment). Permissions are scoped using prefix-level S3 policies, dedicated KMS keys, separate CloudWatch log groups, and dedicated Glue Catalog databases. IAM Access Analyzer can be used to identify unused permissions. Service Control Policies (SCPs) can enforce organization-wide guardrails, such as preventing CloudTrail from being disabled. | Yes – `iam_glue_role` |
| Encryption at Rest | Uses SSE-KMS with a customer-managed KMS key per environment, key rotation enabled, and S3 Bucket Keys to reduce KMS costs. Glue Security Configurations encrypt job outputs and bookmarks. Redshift and Kinesis are encrypted using KMS. SNS topics are also encrypted. | Yes – `s3_data_lake`, `glue_job` |
| Encryption in Transit | S3 bucket policies enforce secure transport (`aws:SecureTransport = false` denied). TLS is enforced for Kinesis/MSK connections using IAM over SASL_SSL. HTTPS-only Application Load Balancer (ALB) with ACM certificates. VPC endpoints keep traffic on the AWS network rather than traversing the public internet. | Yes – Bucket policy and stream job configuration |
| Secrets Management | Database passwords and API keys are stored in AWS Secrets Manager with automatic rotation enabled. Jobs retrieve secrets at runtime rather than storing credentials in code or configuration files. | Yes – `secret_arns` variable; no static credentials stored anywhere |
