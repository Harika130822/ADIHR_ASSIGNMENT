
# 1. Solution Architecture

> https://github.com/Harika130822/ADIHR_ASSIGNMENT/blob/main/Assignment1/Architecture.drawio

> <img width="706" height="739" alt="image" src="https://github.com/user-attachments/assets/d5c56adb-6604-4e48-b2cf-748444421c55" />

> <img width="729" height="779" alt="image" src="https://github.com/user-attachments/assets/1d0fb832-6c51-43a3-8c18-4d2a5f3463a1" />

# Data Ingestion and Data Lake Design

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

> <img width="526" height="742" alt="image" src="https://github.com/user-attachments/assets/4f022b4f-b1cd-434c-9285-98a751c211b9" />

> <img width="541" height="307" alt="image" src="https://github.com/user-attachments/assets/436f6fe6-c439-4a95-aca3-4c078a7d094c" />


> Small-file management
Small files (thousands of 50 KB files) slow Spark and Athena because each file costs a request and task overhead. Target 128 MB–1 GB per file.
At write time: the ETL calls repartition("year","month","day") before writing, giving one file per daily partition (verified: 120 days → 120 files).
Partition granularity: aggregates partitioned by month, not day.
Streaming: micro-batches produce many small files — a nightly compaction job rewrites yesterday's stream partition into a few large files (or use Apache Iceberg tables with rewrite_data_files).
Ingestion: Firehose buffering (e.g. 128 MB or 300 s) before writing to S3.
Monitor: S3 Storage Lens / an Athena query on "$path" to count files per partition.
Screenshot for B: the S3 console showing the bucket's layer folders, and one year=/month=/day= partition.



# ETL and Pyspark Implementation

pyspark/transactions_etl.py is one script that runs both on your laptop (--local) and on AWS Glue unchanged. On the sample data it read 34,320 rows, quarantined 832 bad ones, removed 1,286 duplicates, and a rerun with no new data correctly did nothing.

> <img width="531" height="383" alt="image" src="https://github.com/user-attachments/assets/9866a428-68d4-48d6-b01f-60fc75722d02" />

> <img width="526" height="474" alt="image" src="https://github.com/user-attachments/assets/84693eb9-0059-49f1-a7e5-e57208317536" />



# Data Modelling

A star schema with one fact table at the grain of one row per sales transaction, joined to three dimensions by surrogate keys. DDL is in sql/01_star_schema_ddl.sql, the load in sql/02_load_star_schema.sql; the test runner built it from the ETL output with 35,134 fact rows, 800 customers, 5 regions and zero orphan keys.

> <img width="637" height="370" alt="image" src="https://github.com/user-attachments/assets/72f6f842-f5a3-40be-a259-2a82f6dfab9d" />


> <img width="663" height="814" alt="image" src="https://github.com/user-attachments/assets/762b779e-4812-4481-9fc2-994c968a25ba" />


