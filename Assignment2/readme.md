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

> <img width="631" height="590" alt="image" src="https://github.com/user-attachments/assets/f988bfd3-e5cd-42d5-aaee-c9526b4a4b49" />


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

> <img width="835" height="349" alt="image" src="https://github.com/user-attachments/assets/6aba22f5-8681-4f59-8a62-d1d955a8fe63" />

> <img width="887" height="749" alt="image" src="https://github.com/user-attachments/assets/8328ede4-61db-4678-845d-ccf2d88fff8a" />


# 4. Advanced SQL

> <img width="703" height="736" alt="image" src="https://github.com/user-attachments/assets/6621c9ea-7aa2-4a8c-af41-f85532773874" />


# 5. Security & Governance

> <img width="715" height="711" alt="image" src="https://github.com/user-attachments/assets/32d2acb7-3c13-4053-93d3-913171eb6998" />
