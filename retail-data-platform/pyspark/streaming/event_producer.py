"""
Event producer for the streaming pipeline.

  --target files    : writes newline-delimited JSON files into a folder (local testing, no AWS)
  --target kinesis  : PutRecords to a Kinesis Data Stream (needs AWS credentials via IAM/profile)

Partition key = customer_id  -> all events of one customer go to the same shard/partition,
so they stay in order, while thousands of customers spread load evenly across shards.

It deliberately sends: duplicates (producer retries), late events, and malformed messages,
so you can SEE the stream processor handle them.

    python pyspark/streaming/event_producer.py --target files --out samples/stream_in --events 2000
    python pyspark/streaming/event_producer.py --target kinesis --stream retail-transactions --events 500
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

REGIONS = ["North", "South", "East", "West", "Central"]


def make_event(rng, now):
    return {
        "transaction_id": f"RT{uuid.UUID(int=rng.getrandbits(128)).hex[:16].upper()}",
        "customer_id": f"C{rng.randint(1, 800):05d}",
        "region": rng.choice(REGIONS),
        "store_id": f"S{rng.randint(1, 40):03d}",
        "amount": round(rng.uniform(50, 20000), 2),
        "status": rng.choice(["COMPLETED"] * 4 + ["PENDING"]),
        "event_time": (now - timedelta(seconds=rng.randint(0, 600))).strftime("%Y-%m-%dT%H:%M:%S"),
    }


def generate(n, seed=7):
    rng = random.Random(seed)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    events = []
    for _ in range(n):
        e = make_event(rng, now)
        events.append(json.dumps(e))
        r = rng.random()
        if r < 0.03:                                   # producer retry -> exact duplicate
            events.append(json.dumps(e))
        elif r < 0.05:                                 # late event: happened 30 min ago (within watermark)
            late = dict(e, transaction_id=e["transaction_id"] + "L",
                        event_time=(now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S"))
            events.append(json.dumps(late))
        elif r < 0.06:                                 # very late: 5h ago (beyond 2h watermark)
            vlate = dict(e, transaction_id=e["transaction_id"] + "V",
                         event_time=(now - timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%S"))
            events.append(json.dumps(vlate))
        elif r < 0.07:                                 # poison message -> DLQ
            events.append(rng.choice(['{"transaction_id": "BROKEN", amount: }', "not json at all",
                                      json.dumps({"customer_id": "C00001", "amount": 10})]))
    return events


def to_files(events, out, batch_size=500):
    os.makedirs(out, exist_ok=True)
    for i in range(0, len(events), batch_size):
        tmp = os.path.join(out, f".tmp_{i}")
        with open(tmp, "w") as f:
            f.write("\n".join(events[i:i + batch_size]) + "\n")
        os.rename(tmp, os.path.join(out, f"events_{int(time.time()*1000)}_{i}.json"))  # atomic publish
    print(f"wrote {len(events)} events to {out}")


def to_kinesis(events, stream, region):
    import boto3
    kinesis = boto3.client("kinesis", region_name=region)
    for i in range(0, len(events), 500):                # PutRecords max 500 records per call
        batch = events[i:i + 500]
        records = [{"Data": e.encode(), "PartitionKey": _pkey(e)} for e in batch]
        for attempt in range(5):                        # retry only the failed records, with backoff
            resp = kinesis.put_records(StreamName=stream, Records=records)
            if resp["FailedRecordCount"] == 0:
                break
            records = [r for r, res in zip(records, resp["Records"], strict=True) if "ErrorCode" in res]
            time.sleep(min(2 ** attempt * 0.1, 5))
    print(f"sent {len(events)} events to kinesis stream {stream}")


def _pkey(event_json):
    try:
        return json.loads(event_json).get("customer_id") or "unknown"
    except ValueError:
        return "malformed"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", choices=["files", "kinesis"], default="files")
    ap.add_argument("--out", default="samples/stream_in")
    ap.add_argument("--stream")
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--events", type=int, default=2000)
    a = ap.parse_args()
    evts = generate(a.events)
    to_files(evts, a.out) if a.target == "files" else to_kinesis(evts, a.stream, a.region)
