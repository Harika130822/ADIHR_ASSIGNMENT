#!/usr/bin/env bash
# Proves: (1) duplicates across micro-batches are dropped, (2) events older than the
# watermark are dropped, (3) restart resumes from checkpoint (no reprocessing).
set -euo pipefail
cd "$(dirname "$0")/.."
IN=samples/stream_test_in; OUT=samples/data/stream_test
rm -rf "$IN" "$OUT"; mkdir -p "$IN"
NOW=$(date -u +%Y-%m-%dT%H:%M:%S); OLD=$(date -u -d '-5 hours' +%Y-%m-%dT%H:%M:%S)
cat > "$IN/batch1.json" <<J
{"transaction_id":"T1","customer_id":"C1","region":"north","store_id":"S1","amount":100.0,"status":"COMPLETED","event_time":"$NOW"}
{"transaction_id":"T2","customer_id":"C2","region":"south","store_id":"S1","amount":200.0,"status":"COMPLETED","event_time":"$NOW"}
not json at all
J
python pyspark/streaming/stream_processor.py --source files --input $IN --output $OUT --once >/dev/null 2>&1
echo "after batch 1:"; python -c "import duckdb;print(duckdb.sql(\"select transaction_id from read_parquet('$OUT/processed/transactions_stream/**/*.parquet') order by 1\").fetchall())"
cat > "$IN/batch2.json" <<J
{"transaction_id":"T1","customer_id":"C1","region":"north","store_id":"S1","amount":100.0,"status":"COMPLETED","event_time":"$NOW"}
{"transaction_id":"T3_VERY_LATE","customer_id":"C3","region":"east","store_id":"S1","amount":300.0,"status":"COMPLETED","event_time":"$OLD"}
{"transaction_id":"T4","customer_id":"C4","region":"west","store_id":"S1","amount":400.0,"status":"COMPLETED","event_time":"$NOW"}
J
python pyspark/streaming/stream_processor.py --source files --input $IN --output $OUT --once >/dev/null 2>&1
echo "after batch 2 (restart from checkpoint):"
python - <<P
import duckdb
ids=[r[0] for r in duckdb.sql("select transaction_id from read_parquet('$OUT/processed/transactions_stream/**/*.parquet') order by 1").fetchall()]
print(ids)
assert ids == ["T1","T2","T4"], ids          # T1 duplicate dropped, very-late T3 dropped
dlq=duckdb.sql("select error_reason from read_parquet('$OUT/dlq/transactions_stream/**/*.parquet')").fetchall()
print("dlq:", dlq); assert dlq == [("malformed_json",)]
print("PASS: dedup across batches, watermark drop, DLQ, checkpoint restart")
P
