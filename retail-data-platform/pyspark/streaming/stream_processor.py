"""
Real-time transaction-event pipeline (Assignment section E)

    producers --> Kinesis Data Streams / MSK (Kafka) --> THIS Spark Structured Streaming job
                                                          |-> s3://.../processed/transactions_stream/  (clean, deduped)
                                                          |-> s3://.../curated/region_sales_5min/       (windowed aggregates)
                                                          '-> s3://.../dlq/transactions_stream/         (malformed events)

Run it:
  * Locally (no AWS needed) - reads JSON files dropped into a folder by event_producer.py:
        python pyspark/streaming/stream_processor.py --source files --input samples/stream_in --output samples/data/stream --once
  * On Amazon EMR / Glue streaming with MSK:
        --source kafka --bootstrap b-1.msk...:9098 --topic retail.transactions --output s3://retail-data-lake-prod
  * With Kinesis (Glue 4.0+/EMR ship a Kinesis connector; check option names for your version):
        --source kinesis --stream retail-transactions --region ap-south-1 --output s3://...

Key design points (explained in the guide):
  - explicit schema; malformed JSON -> dead-letter queue instead of crashing the stream
  - event-time watermark (2h) bounds state and decides how "late" is too late
  - dropDuplicatesWithinWatermark(transaction_id) handles producer retries (at-least-once in)
  - file sink + checkpoint = exactly-once output files (Spark tracks committed batches)
  - each query has its own checkpoint -> restart resumes from the last committed offsets
"""
import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [retail-stream] %(message)s")
log = logging.getLogger("retail-stream")

EVENT_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("store_id", T.StringType()),
    T.StructField("amount", T.DecimalType(14, 2)),
    T.StructField("status", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
])

WATERMARK = "2 hours"        # events older than (max event_time seen - 2h) are "too late"
WINDOW = "5 minutes"


def read_source(spark, a):
    """Every source is normalised to a single string column `raw` (the JSON payload)."""
    if a.source == "kafka":
        df = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", a.bootstrap)
              .option("subscribe", a.topic)
              .option("startingOffsets", "latest")
              .option("maxOffsetsPerTrigger", a.max_records)     # back-pressure: cap batch size
              .option("failOnDataLoss", "false")
              # MSK IAM auth (no passwords anywhere):
              .option("kafka.security.protocol", "SASL_SSL")
              .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
              .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
              .option("kafka.sasl.client.callback.handler.class",
                      "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
              .load())
        return df.select(F.col("value").cast("string").alias("raw"))
    if a.source == "kinesis":
        df = (spark.readStream.format("kinesis")
              .option("streamName", a.stream)
              .option("endpointUrl", f"https://kinesis.{a.region}.amazonaws.com")
              .option("startingPosition", "LATEST")
              .option("kinesis.executor.maxFetchRecordsPerShard", a.max_records)  # back-pressure
              .load())
        return df.select(F.col("data").cast("string").alias("raw"))
    # local testing: newline-delimited JSON files appearing in a folder
    return (spark.readStream.format("text")
            .option("maxFilesPerTrigger", 5)                    # back-pressure for the file source
            .load(a.input).withColumnRenamed("value", "raw"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["files", "kafka", "kinesis"], default="files")
    ap.add_argument("--input", help="folder of JSON files (files source)")
    ap.add_argument("--bootstrap")
    ap.add_argument("--topic")
    ap.add_argument("--stream")
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--output", required=True, help="data-lake root, local path or s3://bucket")
    ap.add_argument("--max-records", type=int, default=10000)
    ap.add_argument("--once", action="store_true", help="process what is available, then stop (testing / scheduled micro-batch)")
    a = ap.parse_args()

    builder = SparkSession.builder.appName("retail-transactions-stream")
    if a.source == "files":
        builder = builder.master("local[*]").config("spark.sql.shuffle.partitions", "4")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    out = a.output.rstrip("/")
    ckpt = f"{out}/_checkpoints/transactions_stream"           # one checkpoint folder PER query
    trigger = {"availableNow": True} if a.once else {"processingTime": "30 seconds"}

    parsed = (read_source(spark, a)
              .withColumn("evt", F.from_json("raw", EVENT_SCHEMA))
              .withColumn("ingest_time", F.current_timestamp()))

    # ---- 1) Dead-letter queue: unparseable JSON or missing mandatory fields ----
    is_bad = (F.col("evt").isNull() | F.col("evt.transaction_id").isNull()
              | F.col("evt.event_time").isNull() | F.col("evt.amount").isNull())
    dlq = (parsed.filter(is_bad)
           .select("raw", "ingest_time",
                   # PERMISSIVE from_json gives an all-NULL struct for garbage input
                   F.when(F.col("evt").isNull() | F.coalesce(*[F.col(f"evt.{c}") for c in EVENT_SCHEMA.fieldNames()]).isNull(),
                          "malformed_json")
                    .otherwise("missing_required_field").alias("error_reason"),
                   F.to_date("ingest_time").alias("ingest_date")))
    q_dlq = (dlq.writeStream.format("parquet").outputMode("append")
             .option("path", f"{out}/dlq/transactions_stream")
             .option("checkpointLocation", f"{ckpt}/dlq")
             .partitionBy("ingest_date").trigger(**trigger).queryName("dlq").start())

    # ---- 2) Clean events: watermark + dedup on business key ----
    clean = (parsed.filter(~is_bad).select("evt.*", "ingest_time")
             .withColumn("region", F.upper(F.trim("region")))
             .withWatermark("event_time", WATERMARK))
    deduped = clean.dropDuplicatesWithinWatermark(["transaction_id"])   # Spark 3.5+ (Glue 5.0 / EMR 7)
    q_clean = (deduped.withColumn("event_date", F.to_date("event_time"))
               .writeStream.format("parquet").outputMode("append")
               .option("path", f"{out}/processed/transactions_stream")
               .option("checkpointLocation", f"{ckpt}/clean")
               .partitionBy("event_date").trigger(**trigger).queryName("clean").start())

    # ---- 3) Real-time 5-minute region sales (emitted once the window is final) ----
    agg = (deduped.filter(F.col("status") == "COMPLETED")
           .groupBy(F.window("event_time", WINDOW).alias("w"), "region")
           .agg(F.count("*").alias("txn_count"), F.sum("amount").alias("total_sales"))
           .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                   "region", "txn_count", "total_sales",
                   F.to_date("w.start").alias("event_date")))
    q_agg = (agg.writeStream.format("parquet").outputMode("append")
             .option("path", f"{out}/curated/region_sales_5min")
             .option("checkpointLocation", f"{ckpt}/agg")
             .partitionBy("event_date").trigger(**trigger).queryName("agg").start())

    queries = [q_dlq, q_clean, q_agg]
    if a.once:
        for q in queries:
            q.awaitTermination()
        for q in queries:
            p = q.lastProgress or {}
            log.info("query=%s batch=%s inputRows=%s", q.name, p.get("batchId"), p.get("numInputRows"))
    else:
        # Streaming metrics (input rate, processing rate, batch duration) are visible in the Spark UI
        # and are published by Glue/EMR to CloudWatch; alarms are defined in Terraform.
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
