"""
Retail transactions ETL  (raw  ->  processed  ->  curated)

Runs in TWO places with the same code:
  * Locally (for learning / screenshots):   python pyspark/transactions_etl.py --local ...
  * On AWS Glue 4.0/5.0 as a Spark job:     Glue passes --JOB_NAME --source_path ... as job arguments

What it demonstrates (Assignment section C):
  1. Deduplication on the business key (transaction_id), keeping the latest ingested version
  2. Null handling + data-quality rules; bad rows go to a quarantine area, not the bin
  3. Date normalization from mixed formats + derived year / month / day columns
  4. Region-wise and day-wise aggregations
  5. Parquet output partitioned by year/month/day (snappy compressed)
  6. Error handling, structured logging, DQ metrics, and incremental processing (watermark)
"""
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --------------------------------------------------------------------------------------
# Logging: on Glue, stdout/stderr go to CloudWatch Logs (/aws-glue/jobs/output|error)
# --------------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [retail-etl] %(message)s")
log = logging.getLogger("retail-etl")

RAW_SCHEMA = T.StructType([  # explicit schema = no surprises from schema inference
    T.StructField("transaction_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("customer_name", T.StringType()),
    T.StructField("customer_email", T.StringType()),
    T.StructField("region", T.StringType()),
    T.StructField("store_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("quantity", T.StringType()),
    T.StructField("unit_price", T.StringType()),
    T.StructField("amount", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("payment_method", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("transaction_ts", T.StringType()),
    T.StructField("ingest_ts", T.StringType()),
])
# Raw files are read as strings on purpose: a bad value must not crash the read,
# it must be caught by a DQ rule and quarantined.

TS_FORMATS = ["yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ss'Z'"]
VALID_REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
BUSINESS_KEY = "transaction_id"
MAX_REJECT_RATIO = 0.10  # fail the job if >10% of rows are bad -> upstream is broken


# --------------------------------------------------------------------------------------
# Arguments: Glue style (--KEY value) parsed with getResolvedOptions, argparse locally
# --------------------------------------------------------------------------------------
def parse_args():
    keys = ["source_path", "processed_path", "curated_path", "quarantine_path", "state_path"]
    if "--local" not in sys.argv:
        from awsglue.utils import getResolvedOptions  # only available inside Glue
        args = getResolvedOptions(sys.argv, ["JOB_NAME"] + keys)
        args["run_date"] = _optional_glue_arg("run_date")
        args["local"] = False
        return args
    p = argparse.ArgumentParser()
    p.add_argument("--local", action="store_true")
    for k in keys:
        p.add_argument(f"--{k}", required=True)
    p.add_argument("--run_date", default=None, help="process only this ingest_date (YYYY-MM-DD); default = all new partitions")
    a = vars(p.parse_args())
    a["JOB_NAME"] = "retail-transactions-etl-local"
    return a


def _optional_glue_arg(name):
    flag = f"--{name}"
    if flag in sys.argv:
        i = sys.argv.index(flag)
        return sys.argv[i + 1] if i + 1 < len(sys.argv) else None
    return None


def build_spark(local: bool) -> SparkSession:
    b = SparkSession.builder.appName("retail-transactions-etl")
    if local:
        b = b.master("local[*]").config("spark.sql.shuffle.partitions", "8") \
             .config("spark.driver.memory", "2g")
    spark = (b
             # overwrite ONLY the partitions present in the dataframe (idempotent reruns)
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             # strict modern date parser: invalid dates such as 31/02 become NULL
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# --------------------------------------------------------------------------------------
# Incremental processing: a tiny watermark file remembers the last ingest_date processed
# (In Glue you can ALSO enable Job Bookmarks; the watermark makes the logic explicit.)
# --------------------------------------------------------------------------------------
def read_watermark(spark, state_path):
    try:
        row = spark.read.json(f"{state_path}/watermark.json").collect()
        return row[0]["last_ingest_date"] if row else None
    except AnalysisException:  # path does not exist -> first run, no watermark yet
        return None


def write_watermark(spark, state_path, last_ingest_date, metrics):
    payload = dict(metrics, last_ingest_date=last_ingest_date,
                   updated_at=datetime.now(timezone.utc).isoformat())
    spark.createDataFrame([payload]).coalesce(1).write.mode("overwrite").json(f"{state_path}/watermark.json")


# --------------------------------------------------------------------------------------
# Step 1: extract
# --------------------------------------------------------------------------------------
def extract(spark, source_path, run_date, watermark) -> DataFrame:
    df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
          .schema(RAW_SCHEMA).csv(source_path)          # partition column ingest_date is discovered
          .withColumn("source_file", F.input_file_name()))
    if run_date:                                        # explicit backfill / rerun of one day
        df = df.filter(F.col("ingest_date") == run_date)
    elif watermark:                                     # normal incremental run
        df = df.filter(F.col("ingest_date") > watermark)
    return df


# --------------------------------------------------------------------------------------
# Step 2: clean + standardise + data-quality rules
# --------------------------------------------------------------------------------------
def parse_ts(col):
    return F.coalesce(*[F.to_timestamp(col, f) for f in TS_FORMATS])


def standardise(df: DataFrame) -> DataFrame:
    trimmed = [F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))).alias(c)
               if dict(df.dtypes)[c] == "string" else F.col(c) for c in df.columns]
    df = df.select(*trimmed)                            # "" and "   " -> NULL
    return (df
            .withColumn("region", F.upper("region"))
            .withColumn("status", F.upper("status"))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
            .withColumn("amount", F.col("amount").cast("decimal(14,2)"))
            .withColumn("transaction_ts", parse_ts(F.col("transaction_ts")))
            .withColumn("ingest_ts", F.to_timestamp("ingest_ts", "yyyy-MM-dd HH:mm:ss"))
            # null handling with a sensible default where business allows it
            .withColumn("currency", F.coalesce("currency", F.lit("INR")))
            .withColumn("payment_method", F.coalesce("payment_method", F.lit("UNKNOWN"))))


def apply_dq_rules(df: DataFrame):
    """Returns (good_rows, bad_rows). Each bad row carries the list of rules it broke."""
    rules = {
        "missing_transaction_id": F.col("transaction_id").isNull(),
        "missing_customer_id": F.col("customer_id").isNull(),
        "invalid_or_missing_date": F.col("transaction_ts").isNull(),
        "missing_amount": F.col("amount").isNull(),
        "non_positive_amount": F.col("amount") <= 0,
        "invalid_region": F.col("region").isNull() | ~F.col("region").isin(VALID_REGIONS),
    }
    reasons = F.filter(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()]),
                       lambda x: x.isNotNull())  # Spark 3.1+ (works on Glue 4.0 and 5.0)
    df = df.withColumn("dq_errors", reasons)
    good = df.filter(F.size("dq_errors") == 0).drop("dq_errors")
    bad = df.filter(F.size("dq_errors") > 0) \
            .withColumn("rejected_at", F.current_timestamp())
    return good, bad


# --------------------------------------------------------------------------------------
# Step 3: deduplicate on the business key
# --------------------------------------------------------------------------------------
def deduplicate(df: DataFrame) -> DataFrame:
    """Keep exactly one row per transaction_id: the most recently ingested version.
    This removes exact duplicates AND applies late corrections (e.g. PENDING -> COMPLETED)."""
    order = [F.col("ingest_ts").desc_nulls_last()]
    if "source_file" in df.columns:  # tie-breaker: newer file wins
        order.append(F.col("source_file").desc())
    w = Window.partitionBy(BUSINESS_KEY).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def add_date_parts(df: DataFrame) -> DataFrame:
    return (df.withColumn("transaction_date", F.to_date("transaction_ts"))
              .withColumn("year", F.date_format("transaction_ts", "yyyy"))
              .withColumn("month", F.date_format("transaction_ts", "MM"))
              .withColumn("day", F.date_format("transaction_ts", "dd")))


# --------------------------------------------------------------------------------------
# Step 4: merge with what is already in processed/ (late data touches older partitions)
# --------------------------------------------------------------------------------------
def merge_with_existing(spark, new_df: DataFrame, processed_path: str, staging_path: str) -> DataFrame:
    touched = [r["transaction_date"] for r in new_df.select("transaction_date").distinct().collect()]
    try:
        existing = spark.read.parquet(processed_path).filter(F.col("transaction_date").isin(touched))
        merged = existing.unionByName(new_df, allowMissingColumns=True)
    except AnalysisException:  # path does not exist -> first ever run, processed/ is empty
        merged = new_df
    # re-deduplicate so a record re-sent in a later file replaces the old version
    merged = deduplicate(merged)
    # Spark is lazy: never overwrite a folder while still reading from it.
    # Write the merged result to a staging area first, then read it back.
    merged.write.mode("overwrite").parquet(staging_path)
    return spark.read.parquet(staging_path)


# --------------------------------------------------------------------------------------
# Step 5: aggregations for the curated layer
# --------------------------------------------------------------------------------------
def aggregate(df: DataFrame):
    completed = df.filter(F.col("status") == "COMPLETED")
    daily_region = (completed.groupBy("year", "month", "day", "transaction_date", "region")
                    .agg(F.count("*").alias("txn_count"),
                         F.sum("amount").alias("total_sales"),
                         F.round(F.avg("amount"), 2).alias("avg_ticket"),
                         F.countDistinct("customer_id").alias("unique_customers")))
    daily = (completed.groupBy("year", "month", "day", "transaction_date")
             .agg(F.count("*").alias("txn_count"),
                  F.sum("amount").alias("total_sales"),
                  F.countDistinct("customer_id").alias("unique_customers")))
    return daily_region, daily


def write_parquet(df: DataFrame, path: str, partitions, files_per_partition=1):
    # repartition by the partition columns -> roughly N files per folder = no small-file explosion
    (df.repartition(files_per_partition, *partitions) if files_per_partition > 1
     else df.repartition(*partitions)) \
        .write.mode("overwrite").partitionBy(*partitions).parquet(path)


# --------------------------------------------------------------------------------------
def main():
    args = parse_args()
    spark = build_spark(args["local"])
    started = time.time()
    job = None
    if not args["local"]:
        from awsglue.context import GlueContext
        from awsglue.job import Job
        job = Job(GlueContext(spark.sparkContext))
        job.init(args["JOB_NAME"], args)

    try:
        watermark = read_watermark(spark, args["state_path"])
        log.info("Starting run. run_date=%s watermark=%s", args.get("run_date"), watermark)

        raw = extract(spark, args["source_path"], args.get("run_date"), watermark).cache()
        raw_count = raw.count()
        if raw_count == 0:
            log.info("No new data since watermark %s - nothing to do.", watermark)
            return
        max_ingest = raw.agg(F.max("ingest_date")).first()[0]
        max_ingest = str(max_ingest)

        clean = standardise(raw)
        good, bad = apply_dq_rules(clean)
        good_count, bad_count = good.count(), bad.count()
        reject_ratio = bad_count / raw_count
        log.info("Read %d rows | passed DQ %d | quarantined %d (%.2f%%)",
                 raw_count, good_count, bad_count, reject_ratio * 100)

        if bad_count:
            (bad.withColumn("dq_errors", F.concat_ws(",", "dq_errors"))
                .write.mode("append").partitionBy("ingest_date").parquet(args["quarantine_path"]))
        if reject_ratio > MAX_REJECT_RATIO:
            raise RuntimeError(f"Reject ratio {reject_ratio:.1%} exceeds {MAX_REJECT_RATIO:.0%}; "
                               "stopping before bad data reaches processed/")

        deduped = add_date_parts(deduplicate(good))
        dup_removed = good_count - deduped.count()
        log.info("Deduplication on %s removed %d rows", BUSINESS_KEY, dup_removed)

        final = merge_with_existing(spark, deduped.drop("source_file"), args["processed_path"],
                                    f'{args["state_path"]}/staging')
        write_parquet(final, args["processed_path"], ["year", "month", "day"])
        log.info("Wrote processed/ partitions for %d dates", final.select("transaction_date").distinct().count())

        daily_region, daily = aggregate(final)
        write_parquet(daily_region, f'{args["curated_path"]}/daily_region_sales', ["year", "month"])
        write_parquet(daily, f'{args["curated_path"]}/daily_sales', ["year", "month"])

        metrics = {"job": args["JOB_NAME"], "rows_read": raw_count, "rows_valid": good_count,
                   "rows_quarantined": bad_count, "duplicates_removed": dup_removed,
                   "duration_sec": round(time.time() - started, 1)}
        write_watermark(spark, args["state_path"], max_ingest, metrics)
        # Pure-JSON line on stdout -> CloudWatch metric filter { $.event = "DQ_METRICS" }
        # turns rows_quarantined / duplicates_removed into CloudWatch metrics + alarms
        print(json.dumps(dict(metrics, event="DQ_METRICS")), flush=True)
        if job:
            job.commit()
    except Exception:
        log.exception("ETL run FAILED - watermark not advanced, rerun is safe")
        raise  # non-zero exit -> Glue marks run FAILED -> EventBridge/CloudWatch alarm fires
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
